// api/puter.js

const { init } = require("@heyputer/puter.js/src/init.cjs");

async function runPuterAudit({
  systemPrompt,
  userMessage,
  model
}) {
  const authToken = process.env.PUTER_AUTH_TOKEN;

  if (!authToken) {
    const error = new Error(
      "PUTER_AUTH_TOKEN is missing from the server environment"
    );

    error.code = "PUTER_AUTH_MISSING";

    throw error;
  }

  if (!model || typeof model !== "string" || !model.trim()) {
    const error = new Error(
      "Puter model is required"
    );

    error.code = "PUTER_MODEL_MISSING";

    throw error;
  }

  const puter = init(authToken);

  console.log(
    `[PUTER] Sending audit request using model: ${model}`
  );

  try {
    const response = await puter.ai.chat(
      [
        {
          role: "system",
          content: systemPrompt || ""
        },
        {
          role: "user",
          content: userMessage
        }
      ],
      {
        model: model.trim(),
        normalize: true
      }
    );

    const result =
      response?.message?.content ??
      response?.text ??
      "";

    if (
      !result ||
      !String(result).trim()
    ) {
      const error = new Error(
        "Puter returned an empty audit response"
      );

      error.code = "EMPTY_RESPONSE";

      throw error;
    }

    console.log(
      `[PUTER] Audit response received using model: ${model}`
    );

    return {
      result: String(result).trim(),
      truncated: false
    };

    } catch (error) {

    if (
      error?.code === "EMPTY_RESPONSE"
    ) {
      throw error;
    }

    console.error("[PUTER] RAW ERROR:", error);
    console.error("[PUTER] ERROR TYPE:", typeof error);
    console.error("[PUTER] ERROR NAME:", error?.name);
    console.error("[PUTER] ERROR MESSAGE:", error?.message);
    console.error("[PUTER] ERROR STACK:", error?.stack);

    let rawError;

    try {
      rawError = JSON.stringify(error);
    } catch {
      rawError = String(error);
    }

    console.error("[PUTER] ERROR JSON:", rawError);

    const puterError =
      new Error(
        `Puter AI request failed: ${
          error?.message ||
          rawError ||
          String(error)
        }`
      );

    puterError.code =
      "PUTER_ERROR";

    puterError.originalError =
      error;

    throw puterError;
  }
}

module.exports = {
  runPuterAudit
};
