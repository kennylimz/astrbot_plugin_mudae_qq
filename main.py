from astrbot.api.event import filter, AstrMessageEvent
from astrbot.core.star.filter.platform_adapter_type import PlatformAdapterType
from astrbot.api.star import Context, Star, register
from astrbot.api import AstrBotConfig, logger
import astrbot.api.message_components as Comp
import time
from .util.character_manager import CharacterManager
import random
import asyncio

DRAW_MSG_TTL = 45  # seconds to keep draw message records
DRAW_MSG_INDEX_MAX = 300  # max tracked message ids to avoid unbounded growth

class CCB_Plugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.char_manager = CharacterManager()
        self.config = config
        self.super_admins = self.config.super_admins or []
        self.draw_hourly_limit_default = self.config.draw_hourly_limit or 5
        self.claim_cooldown_default = self.config.claim_cooldown or 3600
        self.harem_max_size_default = self.config.harem_max_size or 10
        self.group_cfgs = {}
        self.user_lists = {}
        self.group_locks = {}

    async def initialize(self):
        """可选择实现异步的插件初始化方法，当实例化该插件类之后会自动调用该方法。"""
        chars = self.char_manager.load_characters()
        if not chars:
            raise RuntimeError("无法加载角色数据：characters.json 缺失或格式错误")

    async def get_group_cfg(self, gid):
        if gid not in self.group_cfgs:
            config = await self.get_kv_data(f"{gid}:config", {}) or {}
            self.group_cfgs[gid] = config
        return self.group_cfgs[gid]

    async def put_group_cfg(self, gid, config):
        self.group_cfgs[gid] = config
        await self.put_kv_data(f"{gid}:config", config)

    async def get_user_list(self, gid):
        if gid not in self.user_lists:
            users = await self.get_kv_data(f"{gid}:user_list", [])
            self.user_lists[gid] = set(users)
        return self.user_lists[gid]

    async def put_user_list(self, gid, users):
        self.user_lists[gid] = set(users)
        await self.put_kv_data(f"{gid}:user_list", list(users))

    async def get_group_role(self, event):
        gid = event.get_group_id() or "global"
        uid = event.get_sender_id()
        resp = await event.bot.api.call_action("get_group_member_info", group_id=gid, user_id=uid)
        return resp.get("role", None)

    def _get_group_lock(self, gid):
        lock = self.group_locks.get(gid)
        if lock is None:
            lock = asyncio.Lock()
            self.group_locks[gid] = lock
        return lock

    @filter.platform_adapter_type(PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_group_notice(self, event: AstrMessageEvent):
        '''用户回应抽卡结果和交换请求的处理器'''
        event.should_call_llm(True)
        gid = event.get_group_id()
        if not gid:
            return  # commands are group-only
        uid = event.get_sender_id()
        if uid == event.get_self_id():
            return
        user_set = await self.get_user_list(gid)
        if uid not in user_set:
            user_set.add(uid)
            await self.put_user_list(gid, user_set)

        raw = getattr(event, "message_obj", None)
        raw_body = getattr(raw, "raw_message", None)
        if isinstance(raw_body, dict):
            post_type = raw_body.get("post_type")
        else:
            post_type = getattr(raw_body, "post_type", None)

        if post_type == "notice":
            if isinstance(raw_body, dict):
                notice_type = raw_body.get("notice_type")
            else:
                notice_type = getattr(raw_body, "notice_type", None)
            if notice_type == "group_msg_emoji_like":
                # stop further pipeline (including default LLM) for notice events
                async for result in self.handle_emoji_like_notice(event):
                    yield result

    async def handle_emoji_like_notice(self, event: AstrMessageEvent):
        '''用户回应抽卡结果和交换请求的处理器'''
        raw = getattr(event, "message_obj", None)
        raw_body = getattr(raw, "raw_message", None)
        notice_type = raw_body.get("notice_type") if isinstance(raw_body, dict) else getattr(raw_body, "notice_type", None)
        emoji_user = event.get_sender_id()
        msg_id = raw_body.get("message_id") if isinstance(raw_body, dict) else getattr(raw_body, "message_id", None)
        now_ts = time.time()
        gid = event.get_group_id() or "global"

        if notice_type == "group_msg_emoji_like" and emoji_user:
            if msg_id:
                draw_msg = await self.get_kv_data(f"{gid}:draw_msg:{msg_id}", None)
                if draw_msg:
                    async for res in self.handle_claim(event):
                        yield res
                    return
                exchange_req = await self.get_kv_data(f"{gid}:exchange_req:{msg_id}", None)
                if exchange_req:
                    if str(emoji_user) != str(exchange_req.get("to_uid")):
                        return
                    await self.delete_kv_data(f"{gid}:exchange_req:{msg_id}")
                    ts = float(exchange_req.get("ts", 0) or 0)
                    idx_key = f"{gid}:exchange_req_index"
                    idx = await self.get_kv_data(idx_key, [])
                    new_idx = [item for item in idx if not (isinstance(item, dict) and item.get("id") == msg_id)]
                    if len(new_idx) != len(idx):
                        await self.put_kv_data(idx_key, new_idx)
                    if ts and (now_ts - ts > DRAW_MSG_TTL):
                        return
                    async for res in self.process_swap(event, exchange_req, msg_id):
                        yield res
                    return

    @filter.command("菜单", alias={"帮助"})
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_help_menu(self, event: AstrMessageEvent):
        '''显示帮助菜单'''
        menu_lines = [
            "普通指令：",
            "菜单/帮助",
            "抽卡/ck",
            "离婚 <角色ID>",
            "最爱 <角色ID>",
            "查询 <角色ID>",
            "搜索 <角色名称>",
            "我的后宫",
            "交换 <我的角色ID> <对方角色ID>",
            "许愿 <角色ID>",
            "愿望单",
            "删除许愿 <角色ID>",
            "================================",
            "管理员指令：",
            "系统设置 <功能> <参数>",
            "清理后宫 <QQ号>",
            "强制离婚 <角色ID>",
            "================================",
            "群主/超管指令：",
            "刷新 <QQ号>",
            "终极轮回"
        ]
        yield event.chain_result([Comp.Plain("\n".join(menu_lines))])
        return

    @filter.command("抽卡", alias={"ck"})
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_draw(self, event: AstrMessageEvent):
        '''抽卡！给结果贴表情来收集'''
        user_id = event.get_sender_id()
        gid = event.get_group_id() or "global"
        key = f"{gid}:{user_id}:draw_status"
        now_ts = time.time()
        config = await self.get_group_cfg(gid)
        limit = config.get("draw_hourly_limit", self.draw_hourly_limit_default)
        now_tm = time.localtime(now_ts)
        bucket = f"{now_tm.tm_year}-{now_tm.tm_yday}-{now_tm.tm_hour}"
        record_bucket, record_count = await self.get_kv_data(key, (None, 0))
        user_set = await self.get_user_list(gid)
        cooldown = config.get("draw_cooldown", 0)

        cooldown = max(cooldown, int(len(user_set)/10))
        if cooldown > 0:
            last_draw_ts = await self.get_kv_data(f"{gid}:{user_id}:last_draw", 0)
            if (now_ts - last_draw_ts) < cooldown:
                # wait_sec = int(cooldown - (now_ts - last_draw_ts))
                # yield event.chain_result([
                #     Comp.At(qq=user_id),
                #     Comp.Plain(f"抽卡冷却中，剩余{wait_sec}秒。")
                # ])
                return
            await self.put_kv_data(f"{gid}:{user_id}:last_draw", now_ts)

        if record_bucket != bucket:
            count = 1
            await self.put_kv_data(key, (bucket, count))
        else:
            count = record_count
            await self.put_kv_data(key, (bucket, count + 1))
            if count >= limit:
                if count == limit:
                    chain = [
                        Comp.At(qq=user_id),
                        Comp.Plain("\u200b\n⚠本小时已达上限⚠")
                    ]
                    
                    yield event.chain_result(chain)
                return
            count += 1

        remaining = limit - count
        wish_list = await self.get_kv_data(f"{gid}:{user_id}:wish_list", [])
        if random.random() < 0.001 and wish_list:
            char_id = random.choice(wish_list)
            character = self.char_manager.get_character_by_id(char_id)
        else:
            character = self.char_manager.get_random_character(limit=config.get('draw_scope', None))
        if not character:
            yield event.plain_result("卡池数据未加载")
            return
        name = character.get("name", "未知角色")
        images = character.get("image") or []
        image_url = random.choice(images) if images else None
        char_id = character.get("id")
        married_to = None
        if char_id is not None:
            claimed_by = await self.get_kv_data(f"{gid}:{char_id}:married_to", None)
            if claimed_by:
                married_to = claimed_by
        wished_by_key = f"{gid}:{char_id}:wished_by"
        wished_by = await self.get_kv_data(wished_by_key, [])
        
        cq_message = []
        if not married_to and wished_by:
            for wisher in wished_by:
                cq_message.append({"type": "at", "data": {"qq": wisher}})
            cq_message.append({"type": "text", "data": {"text": f"已许愿\n{name}"}})
        else:
            cq_message.append({"type": "text", "data": {"text": f"{name}"}})
        if married_to:
            cq_message.append({"type": "text", "data": {"text": "\u200b\n❤已与"}})
            cq_message.append({"type": "at", "data": {"qq": married_to}})
            cq_message.append({"type": "text", "data": {"text": "结婚，勿扰❤"}})
        if image_url:
            cq_message.append({"type": "image", "data": {"file": image_url}})
        
        if remaining == limit-1 and not married_to:
            cq_message.append({"type": "text", "data": {"text": "💡回复任意表情和TA结婚"}})
        if remaining <= 0:
            cq_message.append({"type": "text", "data": {"text": "⚠本小时已达上限⚠"}})

        try:
            # 使用API获取消息ID
            resp = await event.bot.api.call_action("send_group_msg", group_id=event.get_group_id(), message=cq_message)
            msg_id = resp.get("message_id") if isinstance(resp, dict) else None
            if msg_id is not None and not married_to:
                # Maintain a small index; delete expired records
                idx = await self.get_kv_data(f"{gid}:draw_msg_index", [])
                cutoff = now_ts - DRAW_MSG_TTL
                new_idx = []
                if isinstance(idx, list):
                    for item in idx:
                        if not isinstance(item, dict):
                            continue
                        ts_old = item.get("ts", 0)
                        mid_old = item.get("id")
                        if ts_old and ts_old < cutoff and mid_old:
                            await self.delete_kv_data(f"{gid}:draw_msg:{mid_old}")
                            continue
                        new_idx.append(item)
                    idx = new_idx[-(DRAW_MSG_INDEX_MAX - 1) :] if len(new_idx) >= DRAW_MSG_INDEX_MAX else new_idx
                else:
                    idx = []
                idx.append({"id": msg_id, "ts": now_ts})
                await self.put_kv_data(f"{gid}:draw_msg_index", idx)
                await self.put_kv_data(
                    f"{gid}:draw_msg:{msg_id}",
                    {
                        "char_id": str(char_id),
                        "ts": now_ts,
                    },
                )
                await event.bot.api.call_action("set_msg_emoji_like", message_id=msg_id, emoji_id=66, set=True)
        except Exception as e:
            logger.error({"stage": "draw_send_error_bot", "error": repr(e)})

    async def handle_claim(self, event: AstrMessageEvent):
        '''结婚逻辑，给结果贴表情来收集。'''
        gid = event.get_group_id() or "global"
        user_id = event.get_sender_id()
        msg_id = event.message_obj.raw_message.message_id
        # per-user cooldown
        config = await self.get_group_cfg(gid)
        cooldown = config.get("claim_cooldown", self.claim_cooldown_default)
        now_ts = time.time()
        lock = self._get_group_lock(gid)
        async with lock:
            draw_msg = await self.get_kv_data(f"{gid}:draw_msg:{msg_id}", None)
            await self.delete_kv_data(f"{gid}:draw_msg:{msg_id}")
            if not draw_msg:
                return
            ts = draw_msg.get("ts", 0)
            if ts and (now_ts - ts > DRAW_MSG_TTL):
                return
            char_id = draw_msg.get("char_id")
            claimed_by = await self.get_kv_data(f"{gid}:{char_id}:married_to", None)
            if claimed_by:
                return
            last_claim_ts = await self.get_kv_data(f"{gid}:{user_id}:last_claim", 0)
            if (now_ts - last_claim_ts) < cooldown:
                wait_sec = int(cooldown - (now_ts - last_claim_ts))
                wait_min = max(1, (wait_sec + 59) // 60)
                yield event.chain_result([
                    Comp.At(qq=str(user_id)),
                    Comp.Plain(f"结婚冷却中，剩余{wait_min}分钟。")
                ])
                await self.put_kv_data(f"{gid}:draw_msg:{msg_id}", draw_msg)
                return
            
            char_id = draw_msg.get("char_id")
            char = self.char_manager.get_character_by_id(char_id)
            if not char:
                return
            
            # Track per-user marriage list
            marry_list_key = f"{gid}:{user_id}:partners"
            marry_list = await self.get_kv_data(marry_list_key, [])
            if len(marry_list) >= config.get("harem_max_size", self.harem_max_size_default):
                yield event.chain_result([
                    Comp.At(qq=user_id),
                    Comp.Plain(f" 你的后宫已满{config.get('harem_max_size', self.harem_max_size_default)}，无法再结婚。")
                ])
                await self.put_kv_data(f"{gid}:draw_msg:{msg_id}", draw_msg)
                return
            if str(char_id) not in marry_list:
                marry_list.append(str(char_id))
            await self.put_kv_data(marry_list_key, marry_list)
            await self.put_kv_data(f"{gid}:{char_id}:married_to", user_id)
            await self.put_kv_data(f"{gid}:{user_id}:last_claim", now_ts)
            gender = char.get("gender")
            if gender == "女":
                title = "老婆"
            elif gender == "男":
                title = "老公"
            else:
                title = ""
            yield event.chain_result([
                Comp.Reply(id=msg_id),
                Comp.Plain(f"🎉 {char.get('name')} 是 "),
                Comp.At(qq=user_id),
                Comp.Plain(f" 的{title}了！🎉")
            ])

    @filter.command("我的后宫")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_harem(self, event: AstrMessageEvent):
        '''显示收集的人物列表'''
        gid = event.get_group_id() or "global"
        uid = str(event.get_sender_id())
        marry_list_key = f"{gid}:{uid}:partners"
        marry_list = await self.get_kv_data(marry_list_key, [])
        if not marry_list:
            yield event.plain_result("你的后宫空空如也。")
            return
        lines = []
        fav = await self.get_kv_data(f"{gid}:{uid}:fav", None)
        total_heat = 0
        for cid in marry_list:
            char = self.char_manager.get_character_by_id(cid)
            if char is None:
                continue
            heat = char.get("heat") or 0
            total_heat += heat
            fav_mark = ""
            if fav and str(fav) == str(cid):
                fav_mark = "⭐"
            lines.append(f"{fav_mark}{char.get('name')} (ID: {cid})")
        lines.insert(0, f"阵容总人气: {total_heat}")
        chain = [
            Comp.Reply(id=event.message_obj.message_id),
            Comp.Plain("\n".join(lines))
        ]
        yield event.chain_result(chain)

    @filter.command("离婚")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_divorce(self, event: AstrMessageEvent, cid: str | int | None = None):
        '''移除自己与指定角色的婚姻'''
        gid = event.get_group_id() or "global"
        user_id = event.get_sender_id()
        if cid is None or not str(cid).strip().isdigit():
            yield event.plain_result("用法：离婚 <角色ID>")
            return
        cid = int(str(cid).strip())
        marry_list_key = f"{gid}:{user_id}:partners"
        marry_list = await self.get_kv_data(marry_list_key, [])
        cmd_msg_id = event.message_obj.message_id
        if str(cid) not in marry_list:
            yield event.chain_result([
                Comp.Reply(id=cmd_msg_id),
                Comp.Plain(f"结了吗你就离？"),
            ])
            return

        fav = await self.get_kv_data(f"{gid}:{user_id}:fav", None)
        if fav and str(fav) == str(cid):
            await self.delete_kv_data(f"{gid}:{user_id}:fav")
        elif fav is not None and fav not in marry_list:
            await self.delete_kv_data(f"{gid}:{user_id}:fav")

        marry_list = [m for m in marry_list if m != str(cid)]
        await self.put_kv_data(marry_list_key, marry_list)
        await self.delete_kv_data(f"{gid}:{cid}:married_to")
        cname = self.char_manager.get_character_by_id(cid).get("name") or ""
        yield event.chain_result([
            Comp.Reply(id=cmd_msg_id),
            Comp.At(qq=event.get_sender_id()),
            Comp.Plain(f"已与 {cname or cid} 离婚。"),
        ])

    @filter.command("交换")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_exchange(self, event: AstrMessageEvent, my_cid: str | int | None = None, other_cid: str | int | None = None):
        '''向其他用户发起交换请求'''
        gid = event.get_group_id() or "global"
        user_id = event.get_sender_id()
        user_set = await self.get_user_list(gid)
        if my_cid is None or other_cid is None or not str(my_cid).strip().isdigit() or not str(other_cid).strip().isdigit():
            yield event.plain_result("用法：交换 <我的角色ID> <对方角色ID>")
            return
        my_cid = int(str(my_cid).strip())
        other_cid = int(str(other_cid).strip())

        # Validate ownership via char_marry to avoid stale local list
        my_claim_key = f"{gid}:{my_cid}:married_to"
        my_uid = await self.get_kv_data(my_claim_key, None)
        if not my_uid or str(my_uid) != str(user_id):
            yield event.plain_result("你并未与该角色结婚，无法交换。")
            return

        other_claim_key = f"{gid}:{other_cid}:married_to"
        other_uid = await self.get_kv_data(other_claim_key, None)
        if not other_uid or str(other_uid) == str(user_id):
            yield event.plain_result("对方角色未婚，无法交换。")
            return

        if str(other_uid) not in user_set:
            yield event.plain_result("对方角色已不在本群，无法交换。")
            return

        # Prefer existing claim data; avoid loading full character pool
        my_cname = self.char_manager.get_character_by_id(my_cid).get("name") or str(my_cid)
        other_cname = self.char_manager.get_character_by_id(other_cid).get("name") or str(other_cid)

        cq_message = [
            {"type": "reply", "data": {"id": str(event.message_obj.message_id)}},
            {"type": "at", "data": {"qq": user_id}},
            {"type": "text", "data": {"text": f"想用 {my_cname} 向你交换 {other_cname}。\n"}},
            {"type": "at", "data": {"qq": other_uid}},
            {"type": "text", "data": {"text": "若同意，请给此条消息贴表情。"}},
        ]
        try:
            resp = await event.bot.api.call_action("send_group_msg", group_id=event.get_group_id(), message=cq_message)
            msg_id = resp.get("message_id") if isinstance(resp, dict) else None
            if msg_id is not None:
                now_ts = time.time()
                idx_key = f"{gid}:exchange_req_index"
                idx = await self.get_kv_data(idx_key, [])
                cutoff = now_ts - DRAW_MSG_TTL
                new_idx = []
                if isinstance(idx, list):
                    for item in idx:
                        if not isinstance(item, dict):
                            continue
                        ts_old = item.get("ts", 0)
                        mid_old = item.get("id")
                        if ts_old and ts_old < cutoff and mid_old:
                            await self.delete_kv_data(f"{gid}:exchange_req:{mid_old}")
                            continue
                        new_idx.append(item)
                    idx = new_idx[-(DRAW_MSG_INDEX_MAX - 1) :] if len(new_idx) >= DRAW_MSG_INDEX_MAX else new_idx
                else:
                    idx = []
                idx.append({"id": msg_id, "ts": now_ts})
                await self.put_kv_data(idx_key, idx)
                await self.put_kv_data(
                    f"{gid}:exchange_req:{msg_id}",
                    {
                        "from_uid": str(user_id),
                        "to_uid": str(other_uid),
                        "from_cid": str(my_cid),
                        "to_cid": str(other_cid),
                        "ts": time.time(),
                    },
                )
        except Exception as e:
            logger.error({"stage": "exchange_prompt_send_error", "error": repr(e)})
            yield event.plain_result("发送交换请求失败，请稍后再试。")
            return

    async def process_swap(self, event: AstrMessageEvent, req: dict, msg_id):
        gid = event.get_group_id() or "global"
        from_uid = str(req.get("from_uid"))
        to_uid = str(req.get("to_uid"))
        from_cid = str(req.get("from_cid"))
        to_cid = str(req.get("to_cid"))
        user_set = await self.get_user_list(event.get_group_id())
        lock = self._get_group_lock(gid)

        async with lock:
            if not (from_uid in user_set and to_uid in user_set):
                return

            from_claim_key = f"{gid}:{from_cid}:married_to"
            to_claim_key = f"{gid}:{to_cid}:married_to"
            from_marrried_to = await self.get_kv_data(from_claim_key, None)
            to_marrried_to = await self.get_kv_data(to_claim_key, None)

            # Validate ownership
            if not (to_marrried_to and str(to_marrried_to) == to_uid):
                yield event.plain_result("交换失败：对方已不再拥有该角色。")
                return
            if not (from_marrried_to and str(from_marrried_to) == from_uid):
                yield event.plain_result("交换失败：你已不再拥有该角色。")
                return

            from_fav = await self.get_kv_data(f"{gid}:{from_uid}:fav", None)
            to_fav = await self.get_kv_data(f"{gid}:{to_uid}:fav", None)
            if from_fav and str(from_fav) == from_cid:
                await self.delete_kv_data(f"{gid}:{from_uid}:fav")
            if to_fav and str(to_fav) == to_cid:
                await self.delete_kv_data(f"{gid}:{to_uid}:fav")

            from_list_key = f"{gid}:{from_uid}:partners"
            to_list_key = f"{gid}:{to_uid}:partners"
            from_list = await self.get_kv_data(from_list_key, [])
            to_list = await self.get_kv_data(to_list_key, [])

            if from_cid not in from_list or to_cid not in to_list:
                logger.info({"stage": "exchange_fail_missing_role", "msg_id": msg_id})
                yield event.plain_result("交换失败：有人没有对应角色。")
                return

            from_list = [m for m in from_list if m != from_cid]
            to_list = [m for m in to_list if m != to_cid]
            from_list.append(to_cid)
            to_list.append(from_cid)
            await self.put_kv_data(from_list_key, from_list)
            await self.put_kv_data(to_list_key, to_list)

            await self.put_kv_data(to_claim_key, from_uid)
            await self.put_kv_data(from_claim_key, to_uid)
            logger.info({
                "stage": "exchange_success",
                "msg_id": msg_id,
                "from_uid": from_uid,
                "to_uid": to_uid,
                "from_cid": from_cid,
                "to_cid": to_cid,
            })

            from_cname = self.char_manager.get_character_by_id(from_cid).get("name") or str(from_cid)
            to_cname = self.char_manager.get_character_by_id(to_cid).get("name") or str(to_cid)
            yield event.chain_result([
                Comp.Reply(id=str(msg_id)),
                Comp.At(qq=from_uid),
                Comp.Plain(" 与 "),
                Comp.At(qq=to_uid),
                Comp.Plain(f" 已完成交换：{from_cname} ↔ {to_cname}"),
            ])

    @filter.command("最爱")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_favorite(self, event: AstrMessageEvent, cid: str | int | None = None):
        '''将指定角色设为最爱'''
        gid = event.get_group_id() or "global"
        user_id = str(event.get_sender_id())
        if cid is None or not str(cid).strip().isdigit():
            yield event.plain_result("用法：最爱 <角色ID>")
            return
        cid = str(cid).strip()
        marry_list_key = f"{gid}:{user_id}:partners"
        marry_list = await self.get_kv_data(marry_list_key, [])
        target = next((m for m in marry_list if str(m) == str(cid)), None)
        if not target:
            yield event.plain_result("你尚未与该角色结婚！")
            return
        cname = self.char_manager.get_character_by_id(cid).get("name") or ""
        await self.put_kv_data(f"{gid}:{user_id}:fav", cid)
        msg_chain = [
            Comp.Plain("已将 "),
            Comp.Plain(cname or str(cid)),
            Comp.Plain(" 设为你的最爱。"),
        ]
        cmd_msg_id = event.message_obj.message_id
        if cmd_msg_id is not None:
            msg_chain.insert(0, Comp.Reply(id=str(cmd_msg_id)))
        yield event.chain_result(msg_chain)

    @filter.command("许愿")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_wish(self, event: AstrMessageEvent, cid: str | int | None = None):
        '''许愿指定角色'''
        gid = event.get_group_id() or "global"
        user_id = str(event.get_sender_id())
        config = await self.get_group_cfg(gid)
        if cid is None or not str(cid).strip().isdigit():
            yield event.plain_result("用法：许愿 <角色ID>")
            return
        cid = str(cid).strip()
        char = self.char_manager.get_character_by_id(cid)
        if not char:
            yield event.plain_result(f"未找到ID为 {cid} 的角色")
            return
        wish_list_key = f"{gid}:{user_id}:wish_list"
        wish_list = await self.get_kv_data(wish_list_key, [])
        if len(wish_list) >= config.get("harem_max_size", self.harem_max_size_default):
            yield event.chain_result([
                Comp.Reply(id=str(event.message_obj.message_id)),
                Comp.Plain(f"愿望单已满"),
            ])
            return
        if cid not in wish_list:
            wish_list.append(cid)
            await self.put_kv_data(wish_list_key, wish_list)
        wished_by_key = f"{gid}:{cid}:wished_by"
        wished_by = await self.get_kv_data(wished_by_key, [])
        if user_id not in wished_by:
            wished_by.append(user_id)
            await self.put_kv_data(wished_by_key, wished_by)
        yield event.chain_result([
            Comp.Reply(id=str(event.message_obj.message_id)),
            Comp.Plain(f"已许愿 {char.get('name')}"),
        ])

    @filter.command("愿望单")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_wish_list(self, event: AstrMessageEvent):
        '''查看愿望单'''
        gid = event.get_group_id() or "global"
        user_id = str(event.get_sender_id())
        wish_list_key = f"{gid}:{user_id}:wish_list"
        wish_list = await self.get_kv_data(wish_list_key, [])
        if not wish_list:
            yield event.chain_result([
                Comp.Reply(id=str(event.message_obj.message_id)),
                Comp.At(qq=user_id),
                Comp.Plain("你的愿望单为空"),
            ])
            return
        lines = []
        for cid in wish_list:
            married_to = await self.get_kv_data(f"{gid}:{cid}:married_to", None)
            char = self.char_manager.get_character_by_id(cid)
            if char is None:
                continue
            line = f"{char.get('name')}(ID: {cid})"
            if married_to:
                if str(married_to) == user_id:
                    line += "❤️"
                else:
                    line += "💔"
            lines.append(line)
        yield event.chain_result([
            Comp.Reply(id=str(event.message_obj.message_id)),
            Comp.Plain("\n".join(lines)),
        ])

    @filter.command("删除许愿")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_wish_clear(self, event: AstrMessageEvent, cid: str | int | None = None):
        '''从愿望单中删除指定角色'''
        gid = event.get_group_id() or "global"
        user_id = str(event.get_sender_id())
        if cid is None or not str(cid).strip().isdigit():
            yield event.plain_result("用法：删除许愿 <角色ID>")
            return
        cid = str(cid).strip()
        wish_list_key = f"{gid}:{user_id}:wish_list"
        wish_list = await self.get_kv_data(wish_list_key, [])
        wish_list = [x for x in wish_list if str(x) != cid]
        await self.put_kv_data(wish_list_key, wish_list)
        wished_by_key = f"{gid}:{cid}:wished_by"
        wished_by = await self.get_kv_data(wished_by_key, [])
        wished_by = [uid for uid in wished_by if str(uid) != user_id]
        if wished_by:
            await self.put_kv_data(wished_by_key, wished_by)
        else:
            await self.delete_kv_data(wished_by_key)
        yield event.chain_result([
            Comp.Reply(id=str(event.message_obj.message_id)),
            Comp.Plain(f"已从愿望单移除"),
        ])

    @filter.command("查询")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_query(self, event: AstrMessageEvent, cid: str | int | None = None):
        '''查询指定角色的信息'''
        if cid is None:
            yield event.plain_result("用法：查询 <角色ID>")
            return

        cid_str = str(cid).strip()
        if cid_str.isdigit():
            cid_int = int(cid_str)
            char = self.char_manager.get_character_by_id(cid_int)
            if not char:
                yield event.plain_result(f"未找到ID为 {cid_int} 的角色")
                return
            async for res in self.print_character_info(event, char):
                yield res
                return
        else:
            async for res in self.handle_search(event, cid_str):
                yield res
                return

    async def print_character_info(self, event: AstrMessageEvent, char: dict):
        '''打印角色信息'''
        name = char.get("name", "")
        gender = char.get("gender")
        gender_mark = "❓"
        if gender == "男":
            gender_mark = "♂"
        elif gender == "女":
            gender_mark = "♀"
        heat = char.get("heat")
        images = char.get("image") or []
        image_url = random.choice(images) if images else None
        gid = event.get_group_id() or "global"
        married_to = await self.get_kv_data(f"{gid}:{char.get('id')}:married_to", None)
        chain = [Comp.Plain(f"ID: {char.get('id')}\n{name}\n{gender_mark}\n热度: {heat}")]
        if image_url:
            chain.append(Comp.Image.fromURL(image_url))
        if married_to:
            chain.append(Comp.Plain("❤已与 "))
            chain.append(Comp.At(qq=married_to))
            chain.append(Comp.Plain("结婚❤"))
        yield event.chain_result(chain)

    @filter.command("搜索")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_search(self, event: AstrMessageEvent, keyword: str | None = None):
        '''搜索角色'''
        if not keyword:
            yield event.plain_result("用法：搜索 <角色名字/部分名字>")
            return
        keyword = str(keyword).strip()
        matches = self.char_manager.search_characters_by_name(keyword)
        if not matches:
            yield event.plain_result(f"未找到名称包含“{keyword}”的角色")
            return
        if len(matches) == 1:
            char = matches[0]
            async for res in self.print_character_info(event, char):
                yield res
                return
            return
        else:
            top = matches[:10]
            lines = [f"{c.get('name')} (ID: {c.get('id')})" for c in top]
            more = "" if len(matches) <= len(top) else f"\n..."
            yield event.plain_result("\n".join(lines) + more)

    @filter.command("强制离婚")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_force_divorce(self, event: AstrMessageEvent, cid: str | int | None = None):
        '''强制移除指定角色的婚姻，可用于清除坏的数据（管理员专用）'''
        group_role = await self.get_group_role(event)
        if group_role not in ['admin', 'owner'] and str(event.get_sender_id()) not in self.super_admins:
            yield event.plain_result("无权限执行此命令。")
            return
        gid = event.get_group_id() or "global"
        if cid is None or not str(cid).strip().isdigit():
            yield event.plain_result("用法：强制离婚 <角色ID>")
            return
        cid = int(str(cid).strip())
        await self.delete_kv_data(f"{gid}:{cid}:married_to")

        # 遍历所有用户检查坏数据
        users = await self.get_kv_data(f"{gid}:user_list", [])
        for uid in users:
            partners_key = f"{gid}:{uid}:partners"
            marry_list = await self.get_kv_data(partners_key, [])
            if str(cid) in marry_list:
                marry_list = [m for m in marry_list if m != str(cid)]
                await self.put_kv_data(partners_key, marry_list)
                fav = await self.get_kv_data(f"{gid}:{uid}:fav", None)
                if fav and str(fav) == str(cid):
                    await self.delete_kv_data(f"{gid}:{uid}:fav")

        cname = (self.char_manager.get_character_by_id(cid) or {}).get("name") or cid
        yield event.plain_result(f"{cname} 已被强制解除婚约。")

    @filter.command("清理后宫")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_clear_harem(self, event: AstrMessageEvent, uid: str | None = None):
        '''清理指定用户的后宫，最爱会被保留（管理员专用）'''
        group_role = await self.get_group_role(event)
        if group_role not in ['admin', 'owner'] and str(event.get_sender_id()) not in self.super_admins:
            yield event.plain_result("无权限执行此命令。")
            return
        gid = event.get_group_id() or "global"
        lock = self._get_group_lock(gid)
        async with lock:
            if uid is None or not str(uid).strip().isdigit():
                yield event.plain_result("用法：清理后宫 <QQ号>")
                return
            uid = str(uid).strip()
            fav = await self.get_kv_data(f"{gid}:{uid}:fav", None)
            marry_list = await self.get_kv_data(f"{gid}:{uid}:partners", [])
            if not marry_list:
                await self.delete_kv_data(f"{gid}:{uid}:fav")
                await self.delete_kv_data(f"{gid}:{uid}:partners")
                yield event.plain_result(f"{uid} 的后宫为空")
                return
            for cid in marry_list:
                if str(cid) == str(fav):
                    continue
                await self.delete_kv_data(f"{gid}:{cid}:married_to")
            if fav is None:
                await self.delete_kv_data(f"{gid}:{uid}:partners")
            elif fav not in marry_list:
                await self.delete_kv_data(f"{gid}:{uid}:fav")
                await self.delete_kv_data(f"{gid}:{uid}:partners")
            else:
                await self.put_kv_data(f"{gid}:{uid}:partners", [fav])
            yield event.plain_result(f"已清理 {uid} 的后宫")

    @filter.command("系统设置")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_config(self, event: AstrMessageEvent, feature: str | None = None, value: str | None = None):
        '''系统设置（管理员专用）'''
        group_role = await self.get_group_role(event)
        if group_role not in ['admin', 'owner'] and str(event.get_sender_id()) not in self.super_admins:
            yield event.plain_result("无权限执行此命令。")
            return
        config = await self.get_group_cfg(event.get_group_id())
        menu_lines = [
            "用法：",
            f"系统设置 抽卡冷却 [0~600]",
            f"———抽卡冷却（秒） | 当前值: {config.get('draw_cooldown', 0)}",
            "系统设置 抽卡次数 [1~10]",
            f"———每小时抽卡次数 | 当前值: {config.get('draw_hourly_limit', self.draw_hourly_limit_default)}",
            "系统设置 后宫上限 [5~30]",
            f"———后宫人数上限 | 当前值: {config.get('harem_max_size', self.harem_max_size_default)}",
            "系统设置 抽卡范围 [5000~20000]",
            f"———抽卡热度范围 | 当前值: {config.get('draw_scope', '无')}",
        ]
        if feature is None:
            yield event.chain_result([Comp.Plain("\n".join(menu_lines))])
            return
        feature = str(feature).strip()
        if feature == "抽卡冷却":
            if value is None or not str(value).strip().isdigit():
                yield event.plain_result("用法：抽卡冷却 [0~600](秒)")
                return
            time = int(str(value).strip())
            if time < 0:
                time = 0
            if time > 600:
                yield event.plain_result("时间不能超过600秒")
                return
            config["draw_cooldown"] = time
            await self.put_group_cfg(event.get_group_id(), config)
            yield event.plain_result(f"抽卡冷却已设置为{time}秒")
        elif feature == "抽卡次数":
            if value is None or not str(value).strip().isdigit():
                yield event.plain_result("用法：抽卡次数 [1~10]")
                return
            count = int(str(value).strip())
            if count < 1:
                count = 1
            if count > 10:
                yield event.plain_result("次数不能超过10次")
                return
            config["draw_hourly_limit"] = count
            await self.put_group_cfg(event.get_group_id(), config)
            yield event.plain_result(f"每小时抽卡次数已设置为{count}次")
        elif feature == "后宫上限":
            if value is None or not str(value).strip().isdigit():
                yield event.plain_result("用法：后宫上限 [5~30]")
                return
            count = int(str(value).strip())
            if count < 5:
                count = 5
            if count > 30:
                count = 30
            config["harem_max_size"] = count
            await self.put_group_cfg(event.get_group_id(), config)
            yield event.plain_result(f"后宫上限已设置为{count}")
        elif feature == "抽卡范围":
            if value is None or not str(value).strip().isdigit():
                yield event.plain_result("用法：抽卡范围 [>3000]")
                return
            scope = int(str(value).strip())
            if scope < 5000:
                scope = 5000
            elif scope > 20000:
                scope = 20000
            config["draw_scope"] = scope
            await self.put_group_cfg(event.get_group_id(), config)
            yield event.plain_result(f"抽卡范围已设置为热度前{scope}")
        else:
            yield event.chain_result([Comp.Plain("\n".join(menu_lines))]) 

    @filter.command("刷新")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_refresh(self, event: AstrMessageEvent, user_id: str | None = None):
        '''刷新指定用户的抽卡和结婚冷却（群主和超管专用）'''
        group_role = await self.get_group_role(event)
        if group_role not in ['owner'] and str(event.get_sender_id()) not in self.super_admins:
            yield event.plain_result("无权限执行此命令。")
            return
        if user_id is None or not str(user_id).strip():
            yield event.plain_result("用法：刷新 <QQ号>")
            return
        user_id = str(user_id).strip()
        if not user_id:
            yield event.plain_result("用法：刷新 <QQ号>")
            return
        gid = event.get_group_id() or "global"
        await self.delete_kv_data(f"{gid}:{user_id}:draw_status")
        await self.delete_kv_data(f"{gid}:{user_id}:last_claim")
        yield event.plain_result("次数已重置，结婚冷却已清除")

    @filter.command("终极轮回")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_ultimate_reset(self, event: AstrMessageEvent, confirm: str | None = None):
        '''清除本群所有角色婚姻信息（除了最爱角色）（群主和超管专用）'''
        group_role = await self.get_group_role(event)
        if group_role not in ['owner'] and str(event.get_sender_id()) not in self.super_admins:
            yield event.plain_result("无权限执行此命令。")
            return
        if str(confirm) != "确认":
            yield event.plain_result("确定要进行终极轮回吗？此操作将清除本群所有角色婚姻信息（除了最爱角色）。\n如果确定要执行，请使用“终极轮回 确认”")
            return
        gid = event.get_group_id() or "global"
        lock = self._get_group_lock(gid)
        async with lock:
            users = await self.get_kv_data(f"{gid}:user_list", [])
            for uid in users:
                fav = await self.get_kv_data(f"{gid}:{uid}:fav", None)
                marry_list = await self.get_kv_data(f"{gid}:{uid}:partners", [])
                if not marry_list:
                    await self.delete_kv_data(f"{gid}:{uid}:fav")
                    await self.delete_kv_data(f"{gid}:{uid}:partners")
                    continue
                for cid in marry_list:
                    if str(cid) == str(fav):
                        continue
                    await self.delete_kv_data(f"{gid}:{cid}:married_to")
                if fav is None:
                    await self.delete_kv_data(f"{gid}:{uid}:partners")
                elif fav not in marry_list:
                    await self.delete_kv_data(f"{gid}:{uid}:fav")
                    await self.delete_kv_data(f"{gid}:{uid}:partners")
                else:
                    await self.put_kv_data(f"{gid}:{uid}:partners", [fav])
            yield event.plain_result("已清除本群所有角色婚姻信息")

    async def terminate(self):
        """可选择实现异步的插件销毁方法，当插件被卸载/停用时会调用。"""
