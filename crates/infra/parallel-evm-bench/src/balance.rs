//! Opcodes that observe account balances, wrapped to record the observation.

use alloy_primitives::{Address, B256, U256};
use base_common_evm::BaseContext;
use revm::{
    bytecode::opcode::{BALANCE, CALL, CALLCODE, CREATE, CREATE2, SELFBALANCE, SELFDESTRUCT},
    handler::instructions::{EthInstructions, InstructionProvider},
    interpreter::{
        Instruction, InstructionContext, InstructionExecResult,
        instructions::{contract, host},
        interpreter::EthInterpreter,
    },
};

use crate::RecordingDb;

type Context<'a, 'db> = InstructionContext<'a, BaseContext<RecordingDb<'db>>, EthInterpreter>;
type Opcode<'db> = fn(Context<'_, 'db>) -> InstructionExecResult;

/// Replacements for every opcode that reads or checks an account's balance: `BALANCE`,
/// `SELFBALANCE` and `SELFDESTRUCT` observe it absolutely; value-carrying `CALL`, `CALLCODE`,
/// `CREATE` and `CREATE2` check that the executing account holds the value.
#[derive(Debug)]
pub struct BalanceOpcodes;

impl BalanceOpcodes {
    /// Installs the recording opcodes, keeping their static gas costs.
    pub fn install<'db>(
        instructions: &mut EthInstructions<EthInterpreter, BaseContext<RecordingDb<'db>>>,
    ) {
        let opcodes: [(u8, Opcode<'db>); 7] = [
            (BALANCE, Self::balance),
            (SELFBALANCE, Self::selfbalance),
            (SELFDESTRUCT, Self::selfdestruct),
            (CALL, Self::call),
            (CALLCODE, Self::callcode),
            (CREATE, Self::create),
            (CREATE2, Self::create2),
        ];
        for (opcode, instruction) in opcodes {
            let gas = instructions.gas_table()[opcode as usize];
            instructions.insert_instruction(opcode, Instruction::new(instruction), gas);
        }
    }

    fn balance(context: Context<'_, '_>) -> InstructionExecResult {
        let address = context
            .interpreter
            .stack
            .peek(0)
            .map(|word| Address::from_word(B256::from(word.to_be_bytes())));
        let result = host::balance(InstructionContext {
            interpreter: &mut *context.interpreter,
            host: &mut *context.host,
        });
        if let Ok(address) = address {
            context.host.journaled_state.database.observe_balance(address);
        }
        result
    }

    fn selfbalance(context: Context<'_, '_>) -> InstructionExecResult {
        let address = context.interpreter.input.target_address;
        context.host.journaled_state.database.observe_balance(address);
        host::selfbalance(context)
    }

    fn selfdestruct(context: Context<'_, '_>) -> InstructionExecResult {
        let address = context.interpreter.input.target_address;
        context.host.journaled_state.database.observe_balance(address);
        host::selfdestruct(context)
    }

    fn call(mut context: Context<'_, '_>) -> InstructionExecResult {
        Self::require_value(&mut context, 2);
        contract::call::<CALL, _, _>(context)
    }

    fn callcode(mut context: Context<'_, '_>) -> InstructionExecResult {
        Self::require_value(&mut context, 2);
        contract::call::<CALLCODE, _, _>(context)
    }

    fn create(mut context: Context<'_, '_>) -> InstructionExecResult {
        Self::require_value(&mut context, 0);
        contract::create::<false, _, _>(context)
    }

    fn create2(mut context: Context<'_, '_>) -> InstructionExecResult {
        Self::require_value(&mut context, 0);
        contract::create::<true, _, _>(context)
    }

    /// Records the check that the executing account holds the value at stack position `index`.
    /// Balances do not change between the opcode and the frame that performs the check, so the
    /// check sees the current balance. Recording it when the opcode fails earlier (static
    /// context, depth, gas) only over-constrains.
    fn require_value(context: &mut Context<'_, '_>, index: usize) {
        let Ok(value) = context.interpreter.stack.peek(index) else { return };
        if value.is_zero() {
            return;
        }
        let address = context.interpreter.input.target_address;
        let journal = &mut context.host.journaled_state;
        let current: Option<U256> = journal.inner.state.get(&address).map(|a| a.info.balance);
        if let Some(current) = current {
            journal.database.require_balance(address, current, value);
        }
    }
}
