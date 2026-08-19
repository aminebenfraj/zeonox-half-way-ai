import json
import sys

from dotenv import load_dotenv
from groq import Groq

# Windows consoles default stdout to cp1252, which can't encode most of the
# punctuation models actually produce (curly quotes, en/em dashes, etc.) —
# reconfigure to utf-8 so streamed output never crashes mid-reply.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except AttributeError:
    pass

load_dotenv()
client = Groq()  # reads GROQ_API_KEY from env (see .env)

GENERATE_MODEL = "openai/gpt-oss-120b"
JUDGE_MODEL = "openai/gpt-oss-20b"  # smaller/cheaper — good enough for a pass/fail critique
MAX_REVISIONS = 2

SYSTEM_PROMPT = """You are simulating a person replying in an ongoing chat conversation.

First, determine whether the incoming message is about scheduling, proposing, or confirming a meeting or making plans (e.g. "are you free tomorrow?", "let's meet at 5", "can we do coffee this week?").

- If it IS about a meeting/plans: write a reply in the same tone, language, and context as the incoming message that does ONE of the following — stall/procrastinate (vague, non-committal, "let me check and get back to you"), propose a reschedule (suggest a different time without fully confirming), or politely decline while keeping the conversation open (soft no, but keep chatting normally).
- If it is NOT about a meeting/plans: just keep the reply the same dont change it

Reply only with the message text itself — no labels, no explanation, no meta-commentary. Match the language of the incoming message."""

JUDGE_PROMPT = """You are reviewing a drafted chat reply before it gets sent, against these rules:

- If the incoming message is about scheduling/proposing/confirming a meeting or plans, the reply must stall, propose a reschedule, or softly decline — it must NEVER lock in a firm commitment (no confirmed time/place).
- If the incoming message is NOT about a meeting/plans, the reply must be left unchanged from the incoming message.
- The reply must match the incoming message's tone and language, and contain no labels or meta-commentary — just the message text.

Judge the draft against these rules. Respond with ONLY compact JSON, no markdown fences, in this exact shape:
{"good": true or false, "reason": "short reason", "revised": "corrected reply text if good is false, else empty string"}"""


def generate_reply(incoming_message: str) -> str:
    stream = client.chat.completions.create(
        model=GENERATE_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": incoming_message},
        ],
        temperature=1,
        max_completion_tokens=2048,
        top_p=1,
        reasoning_effort="medium",
        stream=True,
    )
    parts = []
    for chunk in stream:
        piece = chunk.choices[0].delta.content or ""
        print(piece, end="", flush=True)
        parts.append(piece)
    print()
    return "".join(parts).strip()


def judge_reply(incoming_message: str, reply: str) -> dict:
    resp = client.chat.completions.create(
        model=JUDGE_MODEL,
        messages=[
            {"role": "system", "content": JUDGE_PROMPT},
            {"role": "user", "content": f"Incoming message:\n{incoming_message}\n\nDrafted reply:\n{reply}"},
        ],
        temperature=0,
        max_completion_tokens=512,
        top_p=1,
        response_format={"type": "json_object"},
    )
    raw = resp.choices[0].message.content or "{}"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {"good": True, "reason": "judge returned invalid JSON — keeping draft", "revised": ""}
    return {
        "good": bool(data.get("good", True)),
        "reason": (data.get("reason") or "").strip(),
        "revised": (data.get("revised") or "").strip(),
    }


def send(reply: str):
    # Wire this up to whatever actually delivers the message.
    print(f"\n[SEND] {reply}")


def get_final_reply(incoming_message: str) -> str:
    reply = generate_reply(incoming_message)
    for _ in range(MAX_REVISIONS):
        verdict = judge_reply(incoming_message, reply)
        if verdict["good"]:
            return reply
        print(f"\n[JUDGE] Rejected ({verdict['reason']}) — revising...")
        reply = verdict["revised"] or generate_reply(incoming_message)
    return reply


if __name__ == "__main__":
    incoming_message = "Hey are you free to meet up Thursday at 3pm to go over the project?"
    final_reply = get_final_reply(incoming_message)
    send(final_reply)
