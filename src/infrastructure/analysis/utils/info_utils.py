class InfoUtils:
    @staticmethod
    def get_user_nickname(config_manager, sender) -> str:
        """
        获取用户昵称

        优先使用 nickname 字段，其次使用 card 字段。
        """
        return (
            sender.get("nickname", "")
            or sender.get("card", "")
            or str(sender.get("user_id", ""))
        )
