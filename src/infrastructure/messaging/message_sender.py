"""
消息发送器 - 基础设施层
提供高层消息发送接口，支持跨平台智能路由。
"""

from ...utils.logger import logger


class MessageSender:
    """
    消息发送器
    封装了 PlatformAdapter 的底层调用，提供更高层的发送接口
    """

    def __init__(self, bot_manager, config_manager):
        self.bot_manager = bot_manager
        self.config_manager = config_manager

    async def send_text(
        self, group_id: str, text: str, platform_id: str | None = None
    ) -> bool:
        """发送文本消息"""
        adapter = self.bot_manager.get_adapter(platform_id)
        if not adapter:
            logger.error(f"[MessageSender] 未找到平台 {platform_id} 的适配器")
            return False
        return await adapter.send_text(group_id, text)

    async def send_image_smart(
        self,
        group_id: str,
        image_url: str,
        caption: str = "",
        platform_id: str | None = None,
    ) -> bool:
        """智能发送图片，支持自动选择适配器"""
        adapter = self.bot_manager.get_adapter(platform_id)
        if not adapter:
            logger.error(f"[MessageSender] 未找到平台 {platform_id} 的适配器")
            return False
        return await adapter.send_image(group_id, image_url, caption)
