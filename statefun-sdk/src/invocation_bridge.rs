///! A bridge between the Protobuf world and the world of the Rust SDK. For use by `Transports`.
use std::collections::HashMap;
use std::time::Duration;

use prost_wkt_types::Any;
use statefun_proto::v2::from_function::EgressMessage;
use statefun_proto::v2::to_function::Request;
use statefun_proto::v2::{from_function, to_function, FromFunction, ToFunction};

use crate::function_registry::FunctionRegistry;
use crate::{Address, Context, EgressIdentifier, FunctionType, InvocationError, StateUpdate};

/// An invokable that takes protobuf `ToFunction` as argument and returns a protobuf `FromFunction`.
pub trait InvocationBridge {
    fn invoke_from_proto(&self, to_function: ToFunction) -> Result<FromFunction, InvocationError>;
}

impl InvocationBridge for FunctionRegistry {
    fn invoke_from_proto(
        &self,
        mut to_function: ToFunction,
    ) -> Result<FromFunction, InvocationError> {
        let mut batch_request = to_function.request;

        log::debug!(
            "FunctionRegistry: processing batch request {:#?}",
            batch_request
        );

        if let Some(r) = batch_request {
            if let Request::Invocation(batch) = r {
                let self_address = batch.target.expect("self_address to be not empty");
                let persisted_values_proto = batch.state;
                let mut persisted_values = parse_persisted_values(&persisted_values_proto);

                // we maintain a map of state updates that we update after every invocation. We maintain
                // this to be able to send back coalesced state updates to the statefun runtime but we
                // also need to update persisted_values so that subsequent invocations also "see" state
                // updates
                let mut coalesced_state_updates: HashMap<String, StateUpdate> = HashMap::new();

                for mut invocation in batch.invocations.into_iter() {
                    let caller_address = invocation.caller.expect("Caller address to be not empty");
                    let argument = invocation.argument.expect("Function to have argument");
                    let context = Context::new(&persisted_values, &self_address, &caller_address);

                    let effects =
                        self.invoke(context.self_address().function_type, context, argument)?;

                    let invocation_messages = invocation_messages(effects.invocations);

                    let delayed_invocation_messages =
                        delayed_invocation_messages(effects.delayed_invocations);

                    let egress_messages = egress_messages(effects.egress_messages);

                    update_state(
                        &mut persisted_values,
                        &mut coalesced_state_updates,
                        effects.state_updates,
                    );
                }

                let state_values = coalesced_state_updates.drain().map(|(_key, value)| value);
                serialize_state_updates(&mut invocation_respose, state_values)?;

                let invocation_respose = statefun_proto::v2::from_function::InvocationResponse {
                    state_mutations: todo!(),
                    outgoing_messages: todo!(),
                    delayed_invocations: todo!(),
                    outgoing_egresses: todo!(),
                };

                return Ok(FromFunction {
                    response: Some(from_function::Response::InvocationResult(
                        invocation_respose,
                    )),
                });
            }
        }

        // TODO: not sure what error returns here.
        Err(InvocationError::FunctionNotFound(FunctionType::new(
            "fml", "fml",
        )))
    }
}

fn parse_persisted_values(
    persisted_values: &[to_function::PersistedValue],
) -> HashMap<String, Any> {
    let mut result = HashMap::new();
    for persisted_value in persisted_values {
        let packed_state = deserialize_state(persisted_value.get_state_value());
        result.insert(persisted_value.get_state_name().to_string(), packed_state);
    }
    result
}

fn deserialize_state(serialized_state: &[u8]) -> Any {
    prost::Message::decode(serialized_state).expect("Could not deserialize state.")
}

fn update_state(
    persisted_state: &mut HashMap<String, Any>,
    coalesced_state: &mut HashMap<String, StateUpdate>,
    state_updates: Vec<StateUpdate>,
) {
    for state_update in state_updates {
        match state_update {
            StateUpdate::Delete(name) => {
                persisted_state.remove(&name);
                coalesced_state.insert(name.clone(), StateUpdate::Delete(name.clone()));
            }
            StateUpdate::Update(name, state) => {
                persisted_state.insert(name.clone(), state.clone());
                coalesced_state.insert(
                    name.clone(),
                    StateUpdate::Update(name.clone(), state.clone()),
                );
            }
        }
    }
}

// TODO: document these functions

fn invocation_messages(invocation_messages: Vec<(Address, Any)>) -> Vec<from_function::Invocation> {
    invocation_messages
        .into_iter()
        .map(|(target, argument)| from_function::Invocation {
            target: Some(target.into_proto()),
            argument: Some(argument),
        })
        .collect()
}

fn delayed_invocation_messages(
    delayed_invocation_messages: Vec<(Address, Duration, Any)>,
) -> Vec<from_function::DelayedInvocation> {
    delayed_invocation_messages
        .into_iter()
        .map(
            |(address, duration, argument)| from_function::DelayedInvocation {
                delay_in_ms: duration.as_millis() as i64,
                target: Some(address.into_proto()),
                argument: Some(argument),
            },
        )
        .collect()
}

fn egress_messages(egress_messages: Vec<(EgressIdentifier, Any)>) -> Vec<EgressMessage> {
    egress_messages
        .into_iter()
        .map(|(egress_identifier, argument)| EgressMessage {
            egress_namespace: egress_identifier.namespace,
            egress_type: egress_identifier.name,
            argument: Some(argument),
        })
        .collect()
}

fn serialize_state_updates<T>(
    invocation_response: &mut from_function::InvocationResponse,
    state_updates: T,
) -> Result<(), prost::DecodeError>
where
    T: IntoIterator<Item = StateUpdate>,
{
    for state_update in state_updates {
        match state_update {
            StateUpdate::Delete(name) => {
                let proto_state_update = from_function::PersistedValueMutation {
                    mutation_type: from_function::persisted_value_mutation::MutationType::Delete
                        .into(),
                    state_name: name,
                    state_value: vec![], // TODO: should this be optional?
                };

                invocation_response.state_mutations.push(proto_state_update);
            }
            StateUpdate::Update(name, state) => {
                let proto_state_update = from_function::PersistedValueMutation {
                    mutation_type: from_function::persisted_value_mutation::MutationType::Modify
                        .into(),
                    state_name: name,
                    state_value: state.value,
                };

                invocation_response.state_mutations.push(proto_state_update);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use prost::Message;
    use prost_wkt::MessageSerde;
    use prost_wkt_types::value::Kind;
    use prost_wkt_types::{Any, Value};

    use statefun_proto::v2::from_function::Response;
    use statefun_proto::v2::to_function::Request;
    use statefun_proto::v2::{from_function, to_function, ToFunction};

    use crate::invocation_bridge::{deserialize_state, InvocationBridge};
    use crate::{Address, Effects, EgressIdentifier, FunctionRegistry, FunctionType};
    use anyhow::anyhow;

    const FOO_STATE: &str = "foo";
    const BAR_STATE: &str = "bar";
    const MESSAGE1: &str = "fli";
    const MESSAGE2: &str = "fla";
    const MESSAGE3: &str = "flu";

    // Verifies that all possible fields in a ToFunction are accessible in a function
    #[test]
    fn forward_to_function() -> anyhow::Result<()> {
        let mut registry = FunctionRegistry::new();
        registry.register_fn(function_type(), |context, message: Value| {
            assert_eq!(context.self_address(), self_address());
            assert_eq!(context.caller_address(), caller_address());
            assert_eq!(
                context
                    .get_state::<Value>(FOO_STATE)
                    .expect("State not here."),
                Value::number(0.0)
            );
            assert_eq!(
                context
                    .get_state::<Value>(BAR_STATE)
                    .expect("State not here."),
                Value::number(0.0)
            );

            let mut effects = Effects::new();

            // the test checks against this message to ensure that the function was invoked
            // and all the asserts above were executed
            effects.send(self_address(), message);

            effects
        });

        let to_function = complete_to_function();

        let mut from_function = registry.invoke_from_proto(to_function)?;

        let mut invocation_respose = from_function.response.unwrap();

        if let Response::InvocationResult(resp) = invocation_respose {
            let mut outgoing = resp.outgoing_messages;

            assert_invocation(
                outgoing.remove(0),
                self_address(),
                Value::string(MESSAGE1.to_string()),
            );
            assert_invocation(
                outgoing.remove(0),
                self_address(),
                Value::string(MESSAGE2.to_string()),
            );
            assert_invocation(
                outgoing.remove(0),
                self_address(),
                Value::string(MESSAGE3.to_string()),
            );

            Ok(())
        } else {
            Err(anyhow!("Expected InvocationResult"))
        }
    }

    // Verifies that messages are correctly forwarded to the Protobuf FromFunction
    #[test]
    fn forward_messages_from_function() -> anyhow::Result<()> {
        let mut registry = FunctionRegistry::new();

        registry.register_fn(function_type(), |_context, message: Value| {
            let mut effects = Effects::new();

            effects.send(self_address(), message.clone());

            effects
        });

        let to_function = complete_to_function();
        let mut from_function = registry.invoke_from_proto(to_function)?;

        let mut invocation_respose = from_function.response.unwrap();

        if let Response::InvocationResult(resp) = invocation_respose {
            let mut outgoing = resp.outgoing_messages;

            assert_invocation(
                outgoing.remove(0),
                self_address(),
                Value::string(MESSAGE1.to_string()),
            );
            assert_invocation(
                outgoing.remove(0),
                self_address(),
                Value::string(MESSAGE2.to_string()),
            );
            assert_invocation(
                outgoing.remove(0),
                self_address(),
                Value::string(MESSAGE3.to_string()),
            );

            Ok(())
        } else {
            Err(anyhow!("Expected InvocationResult"))
        }
    }

    // Verifies that delayed messages are correctly forwarded to the Protobuf FromFunction
    #[test]
    fn forward_delayed_messages_from_function() -> anyhow::Result<()> {
        let mut registry = FunctionRegistry::new();
        registry.register_fn(function_type(), |_context, message: Value| {
            let mut effects = Effects::new();

            effects.send_after(caller_address(), Duration::from_secs(5), message.clone());

            effects
        });

        let to_function = complete_to_function();
        let mut from_function = registry.invoke_from_proto(to_function)?;

        let mut invocation_respose = from_function.response.unwrap();

        if let Response::InvocationResult(resp) = invocation_respose {
            let mut delayed = resp.delayed_invocations;

            assert_delayed_invocation(
                delayed.remove(0),
                caller_address(),
                5000,
                Value::string(MESSAGE1.to_string()),
            );
            assert_delayed_invocation(
                delayed.remove(0),
                caller_address(),
                5000,
                Value::string(MESSAGE2.to_string()),
            );
            assert_delayed_invocation(
                delayed.remove(0),
                caller_address(),
                5000,
                Value::string(MESSAGE3.to_string()),
            );
            Ok(())
        } else {
            Err(anyhow!("Expected InvocationResult"))
        }
    }

    // Verifies that egresses are correctly forwarded to the Protobuf FromFunction
    #[test]
    fn forward_egresses_from_function() -> anyhow::Result<()> {
        let mut registry = FunctionRegistry::new();
        registry.register_fn(function_type(), |_context, _message: String| {
            let mut effects = Effects::new();

            effects.egress(
                EgressIdentifier::new("namespace", "name"),
                Value::string("egress".to_string()),
            );

            effects
        });

        let to_function = complete_to_function();
        let mut from_function = registry.invoke_from_proto(to_function)?;

        let mut invocation_respose = from_function.response.unwrap();
        if let Response::InvocationResult(resp) = invocation_respose {
            let mut egresses = resp.outgoing_egresses;

            assert_egress(
                egresses.remove(0),
                "namespace",
                "name",
                Value::string("egress".to_string()),
            );
            assert_egress(
                egresses.remove(0),
                "namespace",
                "name",
                Value::string("egress".to_string()),
            );
            assert_egress(
                egresses.remove(0),
                "namespace",
                "name",
                Value::string("egress".to_string()),
            );

            Ok(())
        } else {
            Err(anyhow!("expected InvocationResponse"))
        }
    }

    // Verifies that state mutations are correctly forwarded to the Protobuf FromFunction
    // #[test]
    // fn forward_state_mutations_from_function() -> anyhow::Result<()> {
    //     let mut registry = FunctionRegistry::new();

    //     registry.register_fn(function_type(), |_context, _message: String| {
    //         let mut effects = Effects::new();

    //         effects.update_state(BAR_STATE, Value::number(42.0));
    //         effects.delete_state(FOO_STATE);

    //         effects
    //     });

    //     let to_function = complete_to_function();
    //     let mut from_function = registry.invoke_from_proto(to_function)?;

    //     let mut invocation_respose = from_function.take_invocation_result();
    //     let state_mutations = invocation_respose.take_state_mutations();

    //     let state_map = to_state_map(state_mutations);
    //     assert_eq!(state_map.len(), 2);

    //     let bar_state = state_map.get(BAR_STATE).unwrap();
    //     let foo_state = state_map.get(FOO_STATE).unwrap();

    //     // state updates are coalesced
    //     assert_state_update(bar_state, BAR_STATE, Value::number(42.0));
    //     assert_state_delete(foo_state, FOO_STATE);

    //     Ok(())
    // }

    fn to_state_map(
        state_mutations: Vec<from_function::PersistedValueMutation>,
    ) -> HashMap<String, from_function::PersistedValueMutation> {
        let mut state_mutations_map = HashMap::new();
        for state_mutation in state_mutations.into_iter() {
            state_mutations_map.insert(state_mutation.state_name.to_string(), state_mutation);
        }
        state_mutations_map
    }

    // Verifies that state mutations are correctly forwarded to the Protobuf FromFunction
    #[test]
    fn state_mutations_available_in_subsequent_invocations() -> anyhow::Result<()> {
        let mut registry = FunctionRegistry::new();

        registry.register_fn(function_type(), |context, message: Value| {
            let state: Value = context.get_state(BAR_STATE).unwrap();

            let mut effects = Effects::new();
            if let Kind::NumberValue(n) = state.kind.unwrap() {
                effects.update_state(BAR_STATE, Value::number(n + 1.0));
                effects.delete_state(FOO_STATE);
                effects
            } else {
                panic!("fml")
            }
        });

        let to_function = complete_to_function();
        let mut from_function = registry.invoke_from_proto(to_function)?;

        let mut invocation_respose = from_function.response.unwrap();
        if let Response::InvocationResult(resp) = invocation_respose {
            let state_mutations = resp.state_mutations;

            let state_map = to_state_map(state_mutations);
            assert_eq!(state_map.len(), 2);

            let bar_state = state_map.get(BAR_STATE).unwrap();
            let foo_state = state_map.get(FOO_STATE).unwrap();

            // state updates are coalesced
            assert_state_update(bar_state, BAR_STATE, Value::number(3.0));
            assert_state_delete(foo_state, FOO_STATE);
            Ok(())
        } else {
            Err(anyhow!("FML"))
        }
    }

    fn assert_invocation(
        invocation: from_function::Invocation,
        expected_address: Address,
        expected_message: Value,
    ) {
        assert_eq!(
            Address::from_proto(&invocation.target.unwrap()),
            expected_address
        );
        assert_eq!(
            unpack_any::<Value>(&invocation.argument.unwrap()),
            &expected_message
        );
    }

    fn assert_delayed_invocation(
        invocation: from_function::DelayedInvocation,
        expected_address: Address,
        expected_delay: i64,
        expected_message: Value,
    ) {
        assert_eq!(
            Address::from_proto(&invocation.target.unwrap()),
            expected_address
        );
        assert_eq!(invocation.delay_in_ms, expected_delay);
        assert_eq!(
            unpack_any::<Value>(&invocation.argument.unwrap()),
            &expected_message
        );
    }

    fn assert_egress(
        egress: from_function::EgressMessage,
        expected_namespace: &str,
        expected_name: &str,
        expected_message: Value,
    ) {
        assert_eq!(egress.egress_namespace, expected_namespace);
        assert_eq!(egress.egress_type, expected_name);
        assert_eq!(
            unpack_any::<Value>(&egress.argument.unwrap()),
            &expected_message
        );
    }

    fn assert_state_update<T: Message + PartialEq + MessageSerde>(
        state_mutation: &from_function::PersistedValueMutation,
        expected_name: &str,
        expected_value: T,
    ) {
        assert_eq!(
            state_mutation.mutation_type(),
            from_function::persisted_value_mutation::MutationType::Modify
        );
        assert_eq!(state_mutation.state_name, expected_name);
        let packed_state: Any = deserialize_state(&state_mutation.state_value);
        assert_eq!(
            *packed_state.try_unpack().unwrap().downcast::<T>().unwrap(),
            expected_value
        )
    }

    fn assert_state_delete(
        state_mutation: &from_function::PersistedValueMutation,
        expected_name: &str,
    ) {
        assert_eq!(
            state_mutation.mutation_type(),
            from_function::persisted_value_mutation::MutationType::Delete
        );
        assert_eq!(state_mutation.state_name, expected_name);
    }

    /// Creates a complete Protobuf ToFunction that contains every possible field/type, including
    /// multiple invocations to test batching behaviour.
    fn complete_to_function() -> ToFunction {
        let invocation_batch = complete_batch_request();
        ToFunction {
            request: Some(Request::Invocation(invocation_batch)),
        }
    }

    fn complete_batch_request() -> to_function::InvocationBatchRequest {
        to_function::InvocationBatchRequest {
            target: Some(self_address().into_proto()),
            state: states(),
            invocations: invocations(),
        }
    }

    fn function_type() -> FunctionType {
        FunctionType::new("namespace", "foo")
    }

    fn self_address() -> Address {
        Address::new(function_type(), "self")
    }

    fn caller_address() -> Address {
        Address::new(function_type(), "caller")
    }

    fn states() -> Vec<to_function::PersistedValue> {
        let mut states = vec![];

        states.push(state(FOO_STATE.to_owned(), 0));
        states.push(state(BAR_STATE.to_owned(), 0));

        states
    }

    fn state(name: String, value: i32) -> to_function::PersistedValue {
        let state_proto_foo = Value::number(value as f64);
        let any_foo = Any::try_pack(state_proto_foo).unwrap();

        to_function::PersistedValue {
            state_name: name,
            state_value: any_foo.value,
        }
    }

    /// It's important to create multiple invocations to test whether state updates can be "seen"
    /// by later invocations in a batch.
    fn invocations() -> Vec<to_function::Invocation> {
        let mut invocations = Vec::new();

        invocations.push(invocation(caller_address(), MESSAGE1));
        invocations.push(invocation(caller_address(), MESSAGE2));
        invocations.push(invocation(caller_address(), MESSAGE3));

        invocations
    }

    fn invocation(caller: Address, argument: &str) -> to_function::Invocation {
        let message = Value::string(argument.to_string());
        let packed_argument = Any::try_pack(message).unwrap();

        to_function::Invocation {
            caller: Some(caller.into_proto()),
            argument: Some(packed_argument),
        }
    }

    fn unpack_any<M: Message + MessageSerde>(any: &Any) -> &M {
        any.try_unpack()
            .unwrap()
            .downcast_ref()
            .expect("Could not unwrap Result")
    }
}
