// api/com.js
// Checkpoint-safe concurrent two-auditor COM orchestrator.

const COM_REVIEW_PROMPT = `
COM CROSS-REVIEW PROTOCOL
You are performing a second-stage adversarial review of a Solidity security audit.
Do not assume either first-pass audit is correct.
Validate findings against source code, reject false positives, re-check your own reasoning,
search for vulnerabilities missed by both auditors, resolve disagreements with concrete code evidence,
and then produce a complete final security assessment in the same structured style as the original audit.
`;

function isDone(v) { return v && v.status === 'complete' && typeof v.result === 'string' && v.result.trim(); }

async function runComBatchItem({ item, contract, runAudit, checkpoint }) {
  const state = item.com || { status: 'phase1', llmA: {}, llmB: {} };
  state.llmA ||= {}; state.llmB ||= {};

  const runPhase = async (side, phase, context) => {
    const bucket = state[side];
    if (isDone(bucket[phase])) return bucket[phase].result;
    bucket[phase] = { ...(bucket[phase] || {}), status: 'running', startedAt: new Date() };
    await checkpoint(state);
    try {
      const result = await runAudit(side, context);
      bucket[phase] = { status: 'complete', result, finishedAt: new Date() };
      await checkpoint(state);
      return result;
    } catch (error) {
      bucket[phase] = { status: 'pending', error: String(error?.message || error), updatedAt: new Date() };
      await checkpoint(state);
      throw error;
    }
  };

  // Phase 1: genuinely concurrent. Each branch checkpoints independently.
  if (!isDone(state.llmA.initial) || !isDone(state.llmB.initial)) {
    state.status = 'phase1'; await checkpoint(state);
    const tasks = [];
    if (!isDone(state.llmA.initial)) tasks.push(runPhase('llmA', 'initial', ''));
    if (!isDone(state.llmB.initial)) tasks.push(runPhase('llmB', 'initial', ''));
    await Promise.all(tasks);
  }

  const a1 = state.llmA.initial.result;
  const b1 = state.llmB.initial.result;

  // Phase 2: genuinely concurrent cross-review.
  if (!isDone(state.llmA.final) || !isDone(state.llmB.final)) {
    state.status = 'phase2'; await checkpoint(state);
    const tasks = [];
    if (!isDone(state.llmA.final)) tasks.push(runPhase('llmA', 'final', `${COM_REVIEW_PROMPT}\n\nYOUR FIRST-PASS AUDIT (A1):\n${a1}\n\nINDEPENDENT AUDITOR FIRST-PASS AUDIT (B1):\n${b1}`));
    if (!isDone(state.llmB.final)) tasks.push(runPhase('llmB', 'final', `${COM_REVIEW_PROMPT}\n\nYOUR FIRST-PASS AUDIT (B1):\n${b1}\n\nINDEPENDENT AUDITOR FIRST-PASS AUDIT (A1):\n${a1}`));
    await Promise.all(tasks);
  }

  state.status = 'complete'; state.completedAt = new Date(); await checkpoint(state);
  return { status: 'completed', com: state };
}

module.exports = { runComBatchItem, COM_REVIEW_PROMPT };
