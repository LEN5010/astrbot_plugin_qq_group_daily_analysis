"""
OneBot v11 平台适配器

支持 NapCat、go-cqhttp、Lagrange 及其他 OneBot 实现。
"""

import asyncio
import base64
import os
from datetime import datetime, timedelta
from typing import Any

import aiohttp

from ....domain.value_objects.platform_capabilities import (
    ONEBOT_V11_CAPABILITIES,
    PlatformCapabilities,
)
from ....domain.value_objects.unified_group import UnifiedGroup, UnifiedMember
from ....domain.value_objects.unified_message import (
    MessageContent,
    MessageContentType,
    UnifiedMessage,
)
from ....utils.logger import logger
from ..base import PlatformAdapter


class OneBotAdapter(PlatformAdapter):
    """
    具体实现：OneBot v11 平台适配器

    支持 NapCat, go-cqhttp, Lagrange 等遵循 OneBot v11 协议的 QQ 机器人框架。
    实现了消息获取、发送、群组管理及头像解析等全套功能。

    Attributes:
        platform_name (str): 平台硬编码标识 'onebot'
        bot_self_ids (list[str]): 机器人自身的 QQ 号列表，用于消息过滤
    """

    platform_name = "onebot"

    # QQ 头像服务 URL 模板
    USER_AVATAR_TEMPLATE = "https://q1.qlogo.cn/g?b=qq&nk={user_id}&s={size}"
    USER_AVATAR_HD_TEMPLATE = (
        "https://q.qlogo.cn/headimg_dl?dst_uin={user_id}&spec={size}&img_type=jpg"
    )
    GROUP_AVATAR_TEMPLATE = "https://p.qlogo.cn/gh/{group_id}/{group_id}/{size}/"

    # OneBot 服务支持的头像尺寸像素
    AVAILABLE_SIZES = (40, 100, 140, 160, 640)

    def __init__(self, bot_instance: Any, config: dict | None = None):
        """
        初始化 OneBot 适配器。
        """
        super().__init__(bot_instance, config)
        # 支持从多个潜在的配置键中提取机器人 ID
        self.bot_self_ids = (
            [str(id) for id in config.get("bot_self_ids", [])] if config else []
        )
        if not self.bot_self_ids and config:
            self.bot_self_ids = [str(id) for id in config.get("bot_qq_ids", [])]

    def _init_capabilities(self) -> PlatformCapabilities:
        """返回预定义的 OneBot v11 能力集。"""
        return ONEBOT_V11_CAPABILITIES

    def _get_nearest_size(self, requested_size: int) -> int:
        """从支持的尺寸列表中找到最接近请求尺寸的一个。"""
        return min(self.AVAILABLE_SIZES, key=lambda x: abs(x - requested_size))

    # ==================== IMessageRepository 实现 ====================

    async def fetch_messages(
        self,
        group_id: str,
        days: int = 1,
        max_count: int = 1000,
        before_id: str | None = None,
        since_ts: int | None = None,
    ) -> list[UnifiedMessage]:
        """
        从 OneBot 后端拉取群组历史消息。
        采用分页拉取策略（参考 portrayal 插件），减少 NapCat/go-cqhttp 单次请求的 CPU 和内存负担。

        Args:
            group_id (str): 群号
            days (int): 拉取过去几天的消息
            max_count (int): 最大拉取条数
            before_id (str, optional): 锚点消息 ID，用于分页回溯
            since_ts (int, optional): 从指定时间戳开始拉取消息（Unix timestamp），优先级高于 days。

        Returns:
            list[UnifiedMessage]: 统一格式的消息列表
        """
        if not hasattr(self.bot, "call_action"):
            return []

        try:
            chunk_size = 100  # 每次拉取 100 条，较为稳健
            all_raw_messages = []

            # 确定回溯的起始时间点
            if since_ts and since_ts > 0:
                start_timestamp = since_ts
            else:
                end_time_dt = datetime.now()
                start_time_dt = end_time_dt - timedelta(days=days)
                start_timestamp = int(start_time_dt.timestamp())

            # 使用 message_seq 或 message_id 进行分页回溯拉取
            current_anchor_id = before_id

            logger.info(
                f"OneBot 开始分页回溯消息: 群 {group_id}, "
                f"起始时间 {datetime.fromtimestamp(start_timestamp).strftime('%Y-%m-%d %H:%M:%S')}, "
                f"上限 {max_count} 条"
            )

            while len(all_raw_messages) < max_count:
                fetch_count = min(chunk_size, max_count - len(all_raw_messages))

                params = {
                    "group_id": int(group_id),
                    "count": fetch_count,
                    "reverseOrder": True,  # 关键：协助分页向上回退拉取历史
                }

                if current_anchor_id:
                    params["message_seq"] = current_anchor_id

                result = await self.bot.call_action("get_group_msg_history", **params)

                if not result or "messages" not in result:
                    logger.debug(
                        f"OneBot 分页拉取：API 调用返回空或无效数据，停止回溯。群: {group_id}"
                    )
                    break

                messages = result.get("messages", [])
                if not messages:
                    logger.debug(
                        f"OneBot 分页拉取：获取到 0 条消息，停止回溯。群: {group_id}"
                    )
                    break

                # 确定该批次中最旧的消息作为下一次回溯的起点
                # 不同 OneBot 实现对 reverseOrder 的处理可能导致结果顺序不同（反映在消息时间戳上）
                # 我们通过比较首尾消息的时间戳，动态识别出本批次中最旧的消息
                first_msg = messages[0]
                last_msg = messages[-1]
                if first_msg.get("time", 0) <= last_msg.get("time", 0):
                    # 正序：首条消息最旧
                    chunk_earliest_msg = first_msg
                else:
                    # 逆序：末条消息最旧
                    chunk_earliest_msg = last_msg

                chunk_earliest_time = chunk_earliest_msg.get("time", 0)

                for raw_msg in messages:
                    msg_time = raw_msg.get("time", 0)
                    msg_id = str(raw_msg.get("message_id", ""))

                    # 基础过滤：去重
                    if any(
                        str(m.get("message_id", "")) == msg_id for m in all_raw_messages
                    ):
                        continue

                    # 身份过滤（排除机器人自己）
                    sender_id = str(raw_msg.get("sender", {}).get("user_id", ""))
                    if sender_id in self.bot_self_ids:
                        continue

                    # 时间范围判定
                    if start_timestamp <= msg_time <= int(datetime.now().timestamp()):
                        all_raw_messages.append(raw_msg)

                # 提取锚点。
                # 优先级: message_seq > real_id > seq > message_id
                # 注意：为了兼容 NapCat (NTQQ) 这种 Message ID 非连续的情况，
                # 以及 LLBot 这种 Sequence 模式，我们统一不进行 -1 偏移。
                # 分页产生的重叠消息将由上方的去重逻辑 (all_raw_messages 循环对比) 自动处理。
                seq_val = (
                    chunk_earliest_msg.get("message_seq")
                    or chunk_earliest_msg.get("real_id")
                    or chunk_earliest_msg.get("seq")
                )
                mid_val = chunk_earliest_msg.get("message_id")

                # 重要修复：取消对 ID 的 -1 位移手动操作。
                # 在 NapCat/NTQQ 中，ID 虽为数字但并不连续。-1 位移会导致“消息不存在”错误。
                # 即使 API 返回的消息包含锚点本身，上方的 deduplication 逻辑也会将其排除，
                # 保证翻页能正常向前推进。
                new_anchor_id = seq_val if seq_val is not None else mid_val

                # 如果消息时间已到达起始点，或者锚点无法继续往前位移，则停止
                if chunk_earliest_time <= start_timestamp:
                    logger.debug(
                        f"OneBot 分页拉取：已到达起始时间 ({start_timestamp})，回溯同步完成。"
                    )
                    break

                if current_anchor_id and str(new_anchor_id) == str(current_anchor_id):
                    logger.debug(
                        "OneBot 分页拉取：消息锚点未发生有效位移，可能已到达历史尽头。"
                    )
                    break

                current_anchor_id = new_anchor_id
                logger.debug(
                    f"OneBot 分页拉取进度: 已获取 {len(all_raw_messages)} 条基础/有效消息，下一次锚点: {current_anchor_id}"
                )

                # 稍微延迟，减缓服务端压力
                await asyncio.sleep(0.05)

            # 统一转换为 UnifiedMessage 并在返回前去重排序
            unified_messages = []
            seen_ids = set()
            for raw_msg in all_raw_messages:
                mid = str(raw_msg.get("message_id", ""))
                if not mid or mid in seen_ids:
                    continue

                unified = self._convert_message(raw_msg, group_id)
                if unified:
                    unified_messages.append(unified)
                    seen_ids.add(mid)

            # 确保最终结果符合时间顺序
            unified_messages.sort(key=lambda m: m.timestamp)

            logger.info(
                f"OneBot 分页拉取完成: 共处理 {len(all_raw_messages)} 条原始消息, 最终有效 {len(unified_messages)} 条"
            )
            return unified_messages

        except Exception as e:
            logger.warning(f"OneBot 分页获取消息失败: {e}")
            return []

    def _convert_message(self, raw_msg: dict, group_id: str) -> UnifiedMessage | None:
        """内部方法：将 OneBot 原生原始消息字典转换为 UnifiedMessage 值对象。"""
        try:
            sender = raw_msg.get("sender", {})
            message_chain = raw_msg.get("message", [])

            # 兼容性处理：如果是字符串格式的 message，转换为列表格式
            if isinstance(message_chain, str):
                message_chain = [{"type": "text", "data": {"text": message_chain}}]

            contents = []
            text_parts = []

            for seg in message_chain:
                seg_type = seg.get("type", "")
                seg_data = seg.get("data", {})

                if seg_type == "text":
                    text = seg_data.get("text", "")
                    text_parts.append(text)
                    contents.append(
                        MessageContent(type=MessageContentType.TEXT, text=text)
                    )

                elif seg_type == "image":
                    # QQ 平台: subType=1 表示表情包，通过 raw_data 传递给下游统计
                    sub_type = seg_data.get("subType", seg_data.get("sub_type"))
                    # 安全地转换为整数，防止非数字值导致异常
                    try:
                        is_sticker = int(sub_type) == 1
                    except (TypeError, ValueError):
                        is_sticker = False
                    # 只在 sub_type 有效时包含在 raw_data 中
                    raw_data: dict[str, Any] = {"summary": seg_data.get("summary", "")}
                    if sub_type is not None:
                        raw_data["sub_type"] = int(sub_type)
                    contents.append(
                        MessageContent(
                            type=MessageContentType.EMOJI
                            if is_sticker
                            else MessageContentType.IMAGE,
                            url=seg_data.get("url", seg_data.get("file", "")),
                            raw_data=raw_data,
                        )
                    )

                elif seg_type == "at":
                    contents.append(
                        MessageContent(
                            type=MessageContentType.AT,
                            at_user_id=str(seg_data.get("qq", "")),
                        )
                    )

                elif seg_type in ("face", "mface", "bface", "sface"):
                    contents.append(
                        MessageContent(
                            type=MessageContentType.EMOJI,
                            emoji_id=str(seg_data.get("id", "")),
                            raw_data={"face_type": seg_type},
                        )
                    )

                elif seg_type == "reply":
                    contents.append(
                        MessageContent(
                            type=MessageContentType.REPLY,
                            raw_data={"reply_id": seg_data.get("id", "")},
                        )
                    )

                elif seg_type == "forward":
                    contents.append(
                        MessageContent(
                            type=MessageContentType.FORWARD, raw_data=seg_data
                        )
                    )

                elif seg_type == "record":
                    contents.append(
                        MessageContent(
                            type=MessageContentType.VOICE,
                            url=seg_data.get("url", seg_data.get("file", "")),
                        )
                    )

                elif seg_type == "video":
                    contents.append(
                        MessageContent(
                            type=MessageContentType.VIDEO,
                            url=seg_data.get("url", seg_data.get("file", "")),
                        )
                    )

                else:
                    contents.append(
                        MessageContent(type=MessageContentType.UNKNOWN, raw_data=seg)
                    )

            # 提取回复 ID
            reply_to = None
            for c in contents:
                if c.type == MessageContentType.REPLY and c.raw_data:
                    reply_to = str(c.raw_data.get("reply_id", ""))
                    break

            return UnifiedMessage(
                message_id=str(raw_msg.get("message_id", "")),
                sender_id=str(sender.get("user_id", "")),
                sender_name=sender.get("nickname", ""),
                sender_card=sender.get("card", "") or None,
                group_id=group_id,
                text_content="".join(text_parts),
                contents=tuple(contents),
                timestamp=raw_msg.get("time", 0),
                platform="onebot",
                reply_to_id=reply_to,
            )

        except Exception as e:
            logger.debug(f"OneBot _convert_message 错误: {e}")
            return None

    def convert_to_raw_format(self, messages: list[UnifiedMessage]) -> list[dict]:
        """
        将统一格式转换回 OneBot v11 原生字典格式。

        使现有业务逻辑逻辑无需重构即可使用新流水。

        Args:
            messages (list[UnifiedMessage]): 统一消息列表

        Returns:
            list[dict]: OneBot 格式的消息字典列表
        """
        raw_messages = []
        for msg in messages:
            message_chain = []
            for content in msg.contents:
                if content.type == MessageContentType.TEXT:
                    message_chain.append(
                        {"type": "text", "data": {"text": content.text or ""}}
                    )
                elif content.type == MessageContentType.IMAGE:
                    message_chain.append(
                        {"type": "image", "data": {"url": content.url or ""}}
                    )
                elif content.type == MessageContentType.AT:
                    message_chain.append(
                        {"type": "at", "data": {"qq": content.at_user_id or ""}}
                    )
                elif content.type == MessageContentType.EMOJI:
                    face_type = (
                        content.raw_data.get("face_type", "face")
                        if content.raw_data
                        else "face"
                    )
                    message_chain.append(
                        {"type": face_type, "data": {"id": content.emoji_id or ""}}
                    )
                elif content.type == MessageContentType.REPLY:
                    reply_id = (
                        content.raw_data.get("reply_id", "") if content.raw_data else ""
                    )
                    message_chain.append({"type": "reply", "data": {"id": reply_id}})
                elif content.type == MessageContentType.FORWARD:
                    message_chain.append(
                        {"type": "forward", "data": content.raw_data or {}}
                    )
                elif content.type == MessageContentType.VOICE:
                    message_chain.append(
                        {"type": "record", "data": {"url": content.url or ""}}
                    )
                elif content.type == MessageContentType.VIDEO:
                    message_chain.append(
                        {"type": "video", "data": {"url": content.url or ""}}
                    )
                elif content.type == MessageContentType.UNKNOWN and content.raw_data:
                    message_chain.append(content.raw_data)

            raw_msg = {
                "message_id": msg.message_id,
                "time": msg.timestamp,
                "sender": {
                    "user_id": msg.sender_id,
                    "nickname": msg.sender_name,
                    "card": msg.sender_card or "",
                },
                "message": message_chain,
                "group_id": msg.group_id,
                "raw_message": msg.text_content,
                "user_id": msg.sender_id,
            }
            raw_messages.append(raw_msg)

        return raw_messages

    # ==================== IMessageSender 实现 ====================

    async def send_text(
        self,
        group_id: str,
        text: str,
        reply_to: str | None = None,
    ) -> bool:
        """
        向群组发送文本消息。

        Args:
            group_id (str): 目标群号
            text (str): 消息内容
            reply_to (str, optional): 引用回复的消息 ID

        Returns:
            bool: 是否发送成功
        """
        try:
            message = [{"type": "text", "data": {"text": text}}]

            if reply_to:
                message.insert(0, {"type": "reply", "data": {"id": reply_to}})

            await self.bot.call_action(
                "send_group_msg",
                group_id=int(group_id),
                message=message,
            )
            return True
        except Exception as e:
            logger.error(f"OneBot 文本发送失败: {e}")
            return False

    async def send_image(
        self,
        group_id: str,
        image_path: str,
        caption: str = "",
    ) -> bool:
        """
        向群组发送图片消息。

        Args:
            group_id (str): 目标群号
            image_path (str): 图片路径或URL
            caption (str): 图片消息的描述文字

        Returns:
            bool: 是否发送成功
        """
        try:
            base_message = []
            if caption:
                base_message.append({"type": "text", "data": {"text": caption}})

            # 默认策略：1) 优先尝试物理路径；2) 路径失败则尝试 Base64
            file_str = image_path
            if not image_path.startswith(("http://", "https://", "base64://")):
                if os.path.isabs(image_path):
                    # 如果是绝对路径且以 / 开头，只需加 file:// 即可构成 file:///
                    if image_path.startswith("/"):
                        file_str = f"file://{image_path}"
                    else:
                        file_str = f"file:///{image_path}"
                else:
                    # 如果是相对路径，转为绝对路径
                    file_str = f"file:///{os.path.abspath(image_path)}"

            try:
                message = list(base_message)
                message.append({"type": "image", "data": {"file": file_str}})
                await self.bot.call_action(
                    "send_group_msg",
                    group_id=int(group_id),
                    message=message,
                )
                return True
            except Exception as e:
                # 如果是网络图片或 Base64 输入，路径备选策略无意义，直接失败
                if image_path.startswith(("http://", "https://", "base64://")):
                    raise e

                error_str = str(e).lower()
                is_potential_success = (
                    "timeout" in error_str
                    or "1200" in error_str
                    or "网络错误" in error_str
                )
                # 关键修复：对疑似成功的超时错误，不做“立即 Base64 补发”。
                # 直接抛到外层统一进入多轮观察，避免同一份报告短时间内连发。
                if is_potential_success:
                    raise e

                logger.warning(f"路径发送图片失败 ({e})，尝试 Base64 图片模式...")
                b64_str = await self._get_base64_from_file(image_path)
                if not b64_str:
                    logger.error(f"Base64 图片模式失败：无法读取图片文件 {image_path}")
                    raise e

                message = list(base_message)
                message.append({"type": "image", "data": {"file": b64_str}})
                await self.bot.call_action(
                    "send_group_msg",
                    group_id=int(group_id),
                    message=message,
                )
                logger.info(f"Base64 图片模式发送图片成功: 群 {group_id}")
                return True

        except Exception as e:
            logger.error(f"OneBot 图片发送最终失败: {e}")
            return False

    async def send_forward_msg(
        self,
        group_id: str,
        nodes: list[dict],
    ) -> bool:
        """
        发送群合并转发消息。
        """
        if not hasattr(self.bot, "call_action"):
            return False

        try:
            # 兼容处理节点中的 uin -> user_id (有些后端偏好 uin)
            for node in nodes:
                if "data" in node:
                    if "user_id" in node["data"] and "uin" not in node["data"]:
                        node["data"]["uin"] = node["data"]["user_id"]

            await self.bot.call_action(
                "send_group_forward_msg",
                group_id=int(group_id),
                messages=nodes,
            )
            return True
        except Exception as e:
            logger.warning(f"[OneBot] 发送合并转发消息失败: {e}")
            return False

    # ==================== IGroupInfoRepository 实现 ====================

    async def get_group_info(self, group_id: str) -> UnifiedGroup | None:
        """获取指定群组的基础元数据。"""
        try:
            result = await self.bot.call_action(
                "get_group_info",
                group_id=int(group_id),
            )

            if not result:
                return None

            return UnifiedGroup(
                group_id=str(result.get("group_id", group_id)),
                group_name=result.get("group_name", ""),
                member_count=result.get("member_count", 0),
                owner_id=str(result.get("owner_id", "")) or None,
                create_time=result.get("group_create_time"),
                platform="onebot",
            )
        except Exception:
            return None

    async def get_group_list(self) -> list[str]:
        """获取当前机器人已加入的所有群组 ID 列表。"""
        try:
            result = await self.bot.call_action("get_group_list")
            return [str(g.get("group_id", "")) for g in result or []]
        except Exception:
            return []

    async def get_member_list(self, group_id: str) -> list[UnifiedMember]:
        """拉取整个群组成员列表。"""
        try:
            result = await self.bot.call_action(
                "get_group_member_list",
                group_id=int(group_id),
            )

            members = []
            for m in result or []:
                members.append(
                    UnifiedMember(
                        user_id=str(m.get("user_id", "")),
                        nickname=m.get("nickname", ""),
                        card=m.get("card", "") or None,
                        role=m.get("role", "member"),
                        join_time=m.get("join_time"),
                    )
                )
            return members
        except Exception:
            return []

    async def get_member_info(
        self,
        group_id: str,
        user_id: str,
    ) -> UnifiedMember | None:
        """拉取特定群成员的详细名片及角色信息。"""
        try:
            result = await self.bot.call_action(
                "get_group_member_info",
                group_id=int(group_id),
                user_id=int(user_id),
            )

            if not result:
                return None

            return UnifiedMember(
                user_id=str(result.get("user_id", user_id)),
                nickname=result.get("nickname", ""),
                card=result.get("card", "") or None,
                role=result.get("role", "member"),
                join_time=result.get("join_time"),
            )
        except Exception:
            return None

    async def _get_base64_from_file(self, file_path: str) -> str | None:
        """
        读取本地文件并返回 Base64 编码字符串。

        Args:
            file_path: 本地文件绝对路径

        Returns:
            str | None: base64://... 格式的字符串，读取失败返回 None
        """
        try:
            import os

            if not os.path.exists(file_path):
                logger.error(f"文件不存在，无法读取 Base64: {file_path}")
                return None

            with open(file_path, "rb") as f:
                data = f.read()
                b64 = base64.b64encode(data).decode("utf-8")
                return f"base64://{b64}"
        except Exception as e:
            logger.error(f"读取文件并转换 Base64 失败: {e}")
            return None

    # ==================== IAvatarRepository 实现 ====================

    async def get_user_avatar_url(
        self,
        user_id: str,
        size: int = 100,
    ) -> str | None:
        """
        拼凑 QQ 官方服务地址获取用户头像。

        Args:
            user_id (str): QQ 号
            size (int): 期望像素大小

        Returns:
            str: 格式化后的 URL
        """
        actual_size = self._get_nearest_size(size)
        # 640 使用 HD 接口更清晰
        if actual_size >= 640:
            return self.USER_AVATAR_HD_TEMPLATE.format(user_id=user_id, size=640)
        return self.USER_AVATAR_TEMPLATE.format(user_id=user_id, size=actual_size)

    async def get_user_avatar_data(
        self,
        user_id: str,
        size: int = 100,
    ) -> str | None:
        """
        通过网络下载头像并转换为 Base64 格式，适用于前端模板直接渲染。
        """
        url = await self.get_user_avatar_url(user_id, size)
        if not url:
            return None

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.read()
                        b64 = base64.b64encode(data).decode("utf-8")
                        content_type = resp.headers.get("Content-Type", "image/png")
                        return f"data:{content_type};base64,{b64}"
        except Exception as e:
            logger.debug(f"OneBot 头像下载失败: {e}")
        return None

    async def get_group_avatar_url(
        self,
        group_id: str,
        size: int = 100,
    ) -> str | None:
        """获取 QQ 群头像地址。"""
        actual_size = self._get_nearest_size(size)
        return self.GROUP_AVATAR_TEMPLATE.format(group_id=group_id, size=actual_size)

    async def batch_get_avatar_urls(
        self,
        user_ids: list[str],
        size: int = 100,
    ) -> dict[str, str | None]:
        """批量映射 QQ 号到其头像 URL 地址。"""
        return {
            user_id: await self.get_user_avatar_url(user_id, size)
            for user_id in user_ids
        }

    async def set_reaction(
        self, group_id: str, message_id: str, emoji: str | int, is_add: bool = True
    ) -> bool:
        """
        OneBot 实现消息回应 (set_msg_emoji_like)。
        支持 Go-CQHTTP, NapCat, Lagrange 等 OneBot 实现。
        """
        try:
            reaction_key = str(emoji)
            emoji_id = {
                "analysis_started": "289",  # 🫣 表情 (表示任务已接收)
                "analysis_done": "124",  # 👌 表情 (表示任务处理完成)
                "🔍": "289",
                "📊": "124",
            }.get(reaction_key, reaction_key)

            await self.bot.call_action(
                "set_msg_emoji_like",
                message_id=int(message_id),
                emoji_id=emoji_id,
                emoji_type="1",  # 还原为最稳定的系统表情类型
                set=is_add,
            )
            return True
        except Exception as e:
            logger.debug(f"OneBot set_reaction 失败 (API 可能不支持): {e}")
            return False
