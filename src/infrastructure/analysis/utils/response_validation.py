from __future__ import annotations


def validate_topic_items(
    data_list: list[dict],
) -> tuple[bool, list[dict] | None, str | None]:
    normalized: list[dict] = []
    for index, item in enumerate(data_list, start=1):
        if not isinstance(item, dict):
            return False, None, f"topic[{index}] is not object"

        extra_keys = set(item) - {"topic", "contributors", "detail"}
        if extra_keys:
            return False, None, f"topic[{index}] has extra keys: {sorted(extra_keys)}"

        topic = str(item.get("topic", "")).strip()
        detail = str(item.get("detail", "")).strip()
        contributors_raw = item.get("contributors")
        if not topic or not detail:
            return False, None, f"topic[{index}] missing topic/detail"
        if not isinstance(contributors_raw, list):
            return False, None, f"topic[{index}] contributors must be array"

        contributors = [str(value).strip() for value in contributors_raw]
        contributors = [value for value in contributors if value]
        normalized.append(
            {
                "topic": topic,
                "contributors": contributors,
                "detail": detail,
            }
        )

    return True, normalized, None


def validate_golden_quote_items(
    data_list: list[dict],
) -> tuple[bool, list[dict] | None, str | None]:
    normalized: list[dict] = []
    for index, item in enumerate(data_list, start=1):
        if not isinstance(item, dict):
            return False, None, f"quote[{index}] is not object"

        extra_keys = set(item) - {"content", "sender", "reason"}
        if extra_keys:
            return False, None, f"quote[{index}] has extra keys: {sorted(extra_keys)}"

        content = str(item.get("content", "")).strip()
        sender = str(item.get("sender", "")).strip()
        reason = str(item.get("reason", "")).strip()
        if not content or not sender or not reason:
            return False, None, f"quote[{index}] missing content/sender/reason"

        normalized.append(
            {
                "content": content,
                "sender": sender,
                "reason": reason,
            }
        )

    return True, normalized, None
