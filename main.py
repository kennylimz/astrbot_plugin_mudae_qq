from astrbot.api.event import filter, AstrMessageEvent
from astrbot.core.star.filter.platform_adapter_type import PlatformAdapterType
from astrbot.api.star import Context, Star, register
from astrbot.api import AstrBotConfig, logger
from astrbot.core.utils.astrbot_path import get_astrbot_data_path
import astrbot.api.message_components as Comp
import os
import time
import aiohttp
from .util.character_manager import CharacterManager
import random
import asyncio
import datetime
import re

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
        self.custom_images_limit_default = self.config.custom_images_limit or 5
        self.group_cfgs = {}
        self.user_lists = {}
        self.group_locks = {}
        self.plugin_data_path = f"{get_astrbot_data_path()}/plugin_data/astrbot_plugin_mudae_qq"

    async def initialize(self):
        """可选择实现异步的插件初始化方法，当实例化该插件类之后会自动调用该方法。"""
        if not os.path.exists(self.plugin_data_path):
            os.makedirs(self.plugin_data_path, exist_ok=True)
        chars = self.char_manager.load_characters()
        if not chars:
            raise RuntimeError("无法加载角色数据：characters.json 缺失或格式错误")
        bonds = self.char_manager.load_bonds()
        if not bonds:
            raise RuntimeError("无法加载收藏数据：bonds.json 缺失或格式错误")

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

        # 检查是否为notice事件：event.message_obj.raw_message.post_type == "notice"
        if (
                event
                and getattr(event, "message_obj", None)
                and getattr(event.message_obj, "raw_message", None)
                and getattr(event.message_obj.raw_message, "post_type", None) == "notice"
                and getattr(event.message_obj.raw_message, "notice_type", None) == "group_msg_emoji_like"
        ):
            # stop further pipeline (including default LLM) for notice events
            async for result in self.handle_emoji_like_notice(event):
                yield result
            return

    async def handle_emoji_like_notice(self, event: AstrMessageEvent):
        '''用户回应抽卡结果和交换请求的处理器'''
        emoji_user = event.get_sender_id()
        msg_id = event.message_obj.raw_message.message_id
        now_ts = time.time()
        gid = event.get_group_id() or "global"

        draw_msg = await self.get_kv_data(f"{gid}:draw_msg:{msg_id}", None)
        if draw_msg:
            event.call_llm = True
            async for res in self.handle_claim(event):
                yield res
            return
        exchange_req = await self.get_kv_data(f"{gid}:exchange_req:{msg_id}", None)
        if exchange_req:
            event.call_llm = True
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
        event.call_llm = True
        menu_lines = [
            "普通指令：",
            "菜单/帮助",
            "抽卡/ck",
            "结婚（必须回复抽卡结果）",
            "离婚 <角色ID>",
            "最爱 <角色ID>",
            "查询 <角色ID> [图片序号]",
            "搜索 <角色名称>",
            "我的后宫",
            "我的后宫 <页码>",
            "群排行",
            "添加图片 <角色ID>",
            "清除图片 <角色ID>",
            "交换 <我的角色ID> <对方角色ID>",
            "许愿 <角色ID>",
            "愿望单",
            "删除许愿 <角色ID>",
            "================================",
            "管理员指令：",
            "系统设置 <功能> <参数>",
            "系统设置 可抽卡时间范围 HH:mm-HH:mm",
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
    @filter.platform_adapter_type(PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_draw(self, event: AstrMessageEvent):
        '''抽卡！给结果贴表情来收集'''
        event.call_llm = True
        user_id = event.get_sender_id()
        gid = event.get_group_id() or "global"
        lock = self._get_group_lock(gid)
        async with lock:
            key = f"{gid}:{user_id}:draw_status"
            now_ts = time.time()
            config = await self.get_group_cfg(gid)

            time_range = config.get("draw_time_range")
            if time_range:
                tz = datetime.timezone(datetime.timedelta(hours=8))
                now = datetime.datetime.now(tz)
                current_minutes = now.hour * 60 + now.minute
                start_time, end_time = time_range.split('-')
                start_hour, start_min = map(int, start_time.split(':'))
                end_hour, end_min = map(int, end_time.split(':'))
                start_minutes = start_hour * 60 + start_min
                end_minutes = end_hour * 60 + end_min

                if start_minutes < end_minutes:
                    if not (start_minutes <= current_minutes <= end_minutes):
                        yield event.plain_result(f"现在还不能抽卡哦～，有效抽卡时间为{start_time} ～{end_time}")
                        return
                else:
                    if not (current_minutes >= start_minutes or current_minutes <= end_minutes):
                        yield event.plain_result(f"现在还不能抽卡哦～，有效抽卡时间为{start_time} ～{end_time}")
                        return

            limit = config.get("draw_hourly_limit", self.draw_hourly_limit_default)
            now_tm = time.localtime(now_ts)
            bucket = f"{now_tm.tm_year}-{now_tm.tm_yday}-{now_tm.tm_hour}"
            record_bucket, record_count = await self.get_kv_data(key, (None, 0))
            cooldown = config.get("draw_cooldown", 0)

            cooldown = max(cooldown, 2)
            if cooldown > 0:
                last_draw_ts = await self.get_kv_data(f"{gid}:last_draw", 0)
                if (now_ts - last_draw_ts) < cooldown:
                    return

            if record_bucket != bucket:
                count = 0
            else:
                count = record_count
                if count >= limit:
                    if count == limit:
                        chain = [
                            Comp.At(qq=user_id),
                            Comp.Plain("\u200b\n⚠本小时已达上限⚠")
                        ]
                        yield event.chain_result(chain)
                        await self.put_kv_data(key, (bucket, count + 1))
                    return

            next_count = count + 1
            remaining = limit - next_count
            wish_list = await self.get_kv_data(f"{gid}:{user_id}:wish_list", [])
            if random.random() < 0.003 and wish_list:
                char_id = random.choice(wish_list)
                character = self.char_manager.get_character_by_id(char_id)
            else:
                character = self.char_manager.get_random_character(limit=config.get('draw_scope', None))
            if not character:
                yield event.plain_result("卡池数据未加载")
                return
            name = character.get("name", "未知角色")
            char_id = character.get("id")
            images = character.get("image") or []
            custom_paths = await self.get_kv_data(f"{gid}:{char_id}:custom_images", []) if char_id is not None else []
            custom_paths = [os.path.join(self.plugin_data_path, p) for p in custom_paths]
            pool = images + custom_paths
            image_url = random.choice(pool) if pool else None
            married_to = None
            if char_id is not None:
                claimed_by = await self.get_kv_data(f"{gid}:{char_id}:married_to", None)
                if claimed_by:
                    married_to = claimed_by
            wished_by_key = f"{gid}:{char_id}:wished_by"
            wished_by = await self.get_kv_data(wished_by_key, [])

            cq_message = []
            ntr_chance = config.get("ntr_chance", 10)
            if married_to is not None and random.random() < ntr_chance*(1+len(wished_by)) / 100:
                allow_ntr = True
            else:
                allow_ntr = False
            if wished_by and (allow_ntr or not married_to):
                for wisher in wished_by:
                    cq_message.append({"type": "at", "data": {"qq": wisher}})
                cq_message.append({"type": "text", "data": {"text": f" 已许愿\n{name}"}})
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
                # 使用NapCat的API获取消息ID
                resp = await event.bot.api.call_action("send_group_msg", group_id=event.get_group_id(), message=cq_message)
                msg_id = resp.get("message_id") if isinstance(resp, dict) else None
                await self.put_kv_data(key, (bucket, next_count))
                await self.put_kv_data(f"{gid}:last_draw", now_ts)
                if msg_id is not None and (allow_ntr or not married_to):
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
                    # 使用NapCat的API贴一个表情
                    if allow_ntr:
                        emoji_id = 128046 # 牛头
                    else:
                        emoji_id = 66 # 爱心
                    await event.bot.api.call_action("set_msg_emoji_like", message_id=msg_id, emoji_id=emoji_id, set=True)
            except Exception as e:
                logger.error({"stage": "draw_send_error_bot", "error": repr(e)})

    async def handle_claim(self, event: AstrMessageEvent, msg_id: str | int | None = None):
        '''结婚逻辑，给结果贴表情来收集。msg_id 可选，不传则从 event 取（表情触发时）；传则用做回复结婚时的抽卡消息 id。'''
        event.call_llm = True
        gid = event.get_group_id() or "global"
        user_id = event.get_sender_id()
        if msg_id is None:
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
            char = self.char_manager.get_character_by_id(char_id)
            if not char:
                return
            claimed_by = await self.get_kv_data(f"{gid}:{char_id}:married_to", None)
            if claimed_by == user_id:
                yield event.chain_result([
                    Comp.At(qq=user_id),
                    Comp.Plain(f"\u200b\n{char.get('name')} 保卫成功！")
                ])
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

            # Claim path (NTR or normal): size check first
            marry_list_key = f"{gid}:{user_id}:partners"
            marry_list = await self.get_kv_data(marry_list_key, [])
            harem_max = config.get("harem_max_size", self.harem_max_size_default)
            if len(marry_list) >= harem_max:
                yield event.chain_result([
                    Comp.At(qq=user_id),
                    Comp.Plain(f" 你的后宫已满{harem_max}，无法再结婚。")
                ])
                await self.put_kv_data(f"{gid}:draw_msg:{msg_id}", draw_msg)
                return

            if claimed_by:
                prev_fav = await self.get_kv_data(f"{gid}:{claimed_by}:fav", None)
                if prev_fav is not None and str(prev_fav) == str(char_id):
                    if random.random() < 0.7:
                        yield event.chain_result([
                            Comp.At(qq=user_id),
                            Comp.Plain("\u200b\n失败了！该角色是对方的最爱")
                        ])
                        return
                    else:
                        await self.delete_kv_data(f"{gid}:{claimed_by}:fav")
                # NTR: Delete old relationship, create new (marry_list already fetched and size-checked)
                prev_marry_key = f"{gid}:{claimed_by}:partners"
                prev_marry_list = await self.get_kv_data(prev_marry_key, [])
                prev_marry_list = [m for m in prev_marry_list if m != str(char_id)]
                await self.put_kv_data(prev_marry_key, prev_marry_list)
                await self.delete_kv_data(f"{gid}:{char_id}:married_to")
                # Create new relationship
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
                    Comp.Plain(f"🐮 {char.get('name')} 是 "),
                    Comp.At(qq=user_id),
                    Comp.Plain(f" 的{title}了！🐮"),
                ])
            else:
                # Normal claim (marry_list already fetched and size-checked)
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

    @filter.command("结婚")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_marry(self, event: AstrMessageEvent):
        '''收集角色（回复抽卡消息时等同于贴表情结婚）'''
        event.call_llm = True
        replied_msg_id = None
        for part in (event.message_obj.message or []):
            if isinstance(part, Comp.Reply) and getattr(part, "id", None):
                replied_msg_id = str(part.id)
                break
        if not replied_msg_id:
            return
        gid = event.get_group_id() or "global"
        draw_msg = await self.get_kv_data(f"{gid}:draw_msg:{replied_msg_id}", None)
        if not draw_msg:
            return
        async for res in self.handle_claim(event, msg_id=replied_msg_id):
            yield res

    @filter.command("我的后宫")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_harem(self, event: AstrMessageEvent, page: int = 0):
        '''显示收集的人物列表'''
        event.call_llm = True
        gid = event.get_group_id() or "global"
        uid = str(event.get_sender_id())
        lock = self._get_group_lock(gid)
        async with lock:
            marry_list_key = f"{gid}:{uid}:partners"
            marry_list = await self.get_kv_data(marry_list_key, [])
            if not marry_list:
                yield event.chain_result([
                    Comp.Reply(id=event.message_obj.message_id),
                    Comp.At(qq=uid),
                    Comp.Plain("，你的后宫空空如也。")
                ])
                harem_heats_key = f"{gid}_harem_heats"
                harem_heats = await self.get_kv_data(harem_heats_key, {}) or {}
                if uid in harem_heats:
                    del harem_heats[uid]
                await self.put_kv_data(harem_heats_key, harem_heats)
                return
            lines = []
            per_page = 10
            fav = await self.get_kv_data(f"{gid}:{uid}:fav", None)
            bond_status_list = self.char_manager.get_bond_collection_status(marry_list)
            char_bond_ratio = {}
            for name, owned, total, ratio, owned_cids in bond_status_list:
                for cid in owned_cids:
                    char_bond_ratio[cid] = max(char_bond_ratio.get(cid, 1.0), ratio)
            total_heat = 0
            entries = []
            for cid in marry_list:
                char = self.char_manager.get_character_by_id(cid)
                if char is None:
                    continue
                base_heat = float(char.get("heat") or 0)
                wished_by = await self.get_kv_data(f"{gid}:{cid}:wished_by", [])
                num_wishers = len(wished_by)
                bond_ratio = char_bond_ratio.get(int(cid), 1.0)
                effective_heat = base_heat * (1.1 ** num_wishers) * bond_ratio
                heat_int = int(round(effective_heat))
                total_heat += heat_int
                fav_mark = ""
                if fav and str(fav) == str(cid):
                    fav_mark = "⭐"
                line = f"{fav_mark}{char.get('name')} (ID: {cid})"
                if (num_wishers or bond_ratio > 1) and base_heat > 0:
                    pct_increase = (effective_heat - base_heat) / base_heat * 100
                    line += f" (+{pct_increase:.1f}%)"
                entries.append(line)
            harem_heats_key = f"{gid}_harem_heats"
            harem_heats = await self.get_kv_data(harem_heats_key, {}) or {}
            harem_heats[uid] = total_heat
            await self.put_kv_data(harem_heats_key, harem_heats)
            if page == 0:
                sender_name = event.get_sender_name() or event.get_sender_id()
                header_parts = [
                    Comp.Plain(f"{sender_name}的后宫\n总人气: {total_heat}")
                ]
                if fav and str(fav) in marry_list:
                    fav_char = self.char_manager.get_character_by_id(fav)
                    if fav_char:
                        images = fav_char.get("image") or []
                        image_url = random.choice(images) if images else None
                        if image_url:
                            header_parts.insert(0, Comp.Image.fromURL(image_url))
                node_list = [
                    Comp.Node(
                        uin=event.get_self_id(),
                        name=f"{sender_name}的后宫",
                        content=header_parts
                    )
                ]
                for idx in range(0, len(entries), per_page):
                    chunk = entries[idx:idx + per_page]
                    node_list.append(
                        Comp.Node(
                            uin=event.get_self_id(),
                            name=f"{sender_name}的后宫",
                            content=[Comp.Plain("\n".join(chunk))]
                        )
                    )
                if bond_status_list:
                    bond_lines = [
                        f"{name}（{owned}/{total}）（+{(ratio - 1) * 100:.0f}%）"
                        for name, owned, total, ratio, _ in bond_status_list
                    ]
                    node_list.append(
                        Comp.Node(
                            uin=event.get_self_id(),
                            name=f"{sender_name}的后宫",
                            content=[Comp.Plain("\n".join(bond_lines))]
                        )
                    )
                yield event.chain_result([
                    Comp.Nodes(node_list)
                ])
                return
            total_pages = max(1, (len(entries) + per_page - 1) // per_page)
            if page < 1:
                page = 1
            if page > total_pages:
                page = total_pages
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            lines.append(f"阵容总人气: {total_heat}")
            lines.extend(entries[start_idx:end_idx])
            lines.append(f"(第{page}/{total_pages}页)")
            chain = [Comp.Reply(id=event.message_obj.message_id)]
            if fav and str(fav) in marry_list:
                fav_char = self.char_manager.get_character_by_id(fav)
                if fav_char:
                    images = fav_char.get("image") or []
                    image_url = random.choice(images) if images else None
                    if image_url:
                        chain.append(Comp.Image.fromURL(image_url))
            chain.append(Comp.Plain("\n".join(lines)))
            yield event.chain_result(chain)

    @filter.command("离婚")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_divorce(self, event: AstrMessageEvent, cid: str | int | None = None):
        '''移除自己与指定角色的婚姻'''
        event.call_llm = True
        gid = event.get_group_id() or "global"
        user_id = event.get_sender_id()
        if cid is None or not str(cid).strip().isdigit():
            yield event.plain_result("用法：离婚 <角色ID>")
            return
        cid = int(str(cid).strip())
        lock = self._get_group_lock(gid)
        async with lock:
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
        event.call_llm = True
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
        event.call_llm = True
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
                # logger.info({"stage": "exchange_fail_missing_role", "msg_id": msg_id})
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
            # logger.info({
            #     "stage": "exchange_success",
            #     "msg_id": msg_id,
            #     "from_uid": from_uid,
            #     "to_uid": to_uid,
            #     "from_cid": from_cid,
            #     "to_cid": to_cid,
            # })

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
        event.call_llm = True
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
        '''许愿指定角色，稍稍增加概率'''
        event.call_llm = True
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
        event.call_llm = True
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
        event.call_llm = True
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
    async def handle_query(self, event: AstrMessageEvent, cid: str | int | None = None, iid: str | int | None = None):
        '''查询指定角色的信息，可选图片序号 iid（1 到 图片数量）'''
        gid = event.get_group_id() or "global"
        config = await self.get_group_cfg(gid)
        query_cooldown = config.get("query_cooldown", 0)
        if query_cooldown > 0:
            last_query_ts = await self.get_kv_data(f"{gid}:last_query", 0)
            if (time.time() - last_query_ts) < query_cooldown:
                yield event.plain_result(f"查询冷却中，请等待{round(query_cooldown-(time.time()-last_query_ts),1)}秒后重试")
                return
        await self.put_kv_data(f"{gid}:last_query", time.time())
        event.call_llm = True
        if cid is None:
            yield event.plain_result("用法：查询 <角色ID> [图片序号]")
            return

        cid_str = str(cid).strip()
        if cid_str.isdigit():
            cid_int = int(cid_str)
            char = self.char_manager.get_character_by_id(cid_int)
            if not char:
                yield event.plain_result(f"未找到ID为 {cid_int} 的角色")
                return
            async for res in self.print_character_info(event, char, iid=iid):
                yield res
                return
        else:
            async for res in self.handle_search(event, cid_str):
                yield res
                return

    async def print_character_info(self, event: AstrMessageEvent, char: dict, iid: str | int | None = None):
        '''打印角色信息，可选 iid 指定图片序号（1 到 len(pool)，无效则随机）'''
        event.call_llm = True
        name = char.get("name", "")
        gender = char.get("gender")
        gender_mark = "❓"
        if gender == "男":
            gender_mark = "♂"
        elif gender == "女":
            gender_mark = "♀"
        heat = char.get("heat")
        gid = event.get_group_id() or "global"
        char_id = char.get("id")
        images = char.get("image") or []
        custom_paths = await self.get_kv_data(f"{gid}:{char_id}:custom_images", []) if char_id is not None else []
        custom_full = [os.path.join(self.plugin_data_path, p) for p in custom_paths]
        pool = images + custom_full
        image_url = None
        if pool:
            if iid is None or not str(iid).strip().isdigit():
                iid = random.randint(1, len(pool))
            else:
                iid = int(str(iid).strip())
                if iid < 1 or iid > len(pool):
                    iid = random.randint(1, len(pool))
            image_url = pool[iid - 1]
        married_to = await self.get_kv_data(f"{gid}:{char_id}:married_to", None)
        wished_by = await self.get_kv_data(f"{gid}:{char_id}:wished_by", [])
        base_heat = float(heat) if heat is not None else 0
        effective_heat = base_heat * (1.1 ** len(wished_by))
        heat_int = int(round(effective_heat))
        heat_display = str(heat_int)
        if len(wished_by) and base_heat > 0:
            pct_increase = (effective_heat - base_heat) / base_heat * 100
            heat_display += f"（+{pct_increase:.1f}%）"
        bonds = self.char_manager.get_bonds_for_character(char_id) if char_id is not None else []
        header = f"ID: {char_id}\n{name}\n{gender_mark}\n热度：{heat_display}"
        if bonds:
            header += f"\n收藏：{' | '.join(bonds)}"
        chain = [Comp.Plain(header)]
        if image_url:
            if image_url.startswith("http://") or image_url.startswith("https://"):
                chain.append(Comp.Image.fromURL(image_url))
            else:
                chain.append(Comp.Image.fromFileSystem(image_url))
            chain.append(Comp.Plain(f"\n({iid}/{len(pool)})"))
        if married_to:
            try:
                user_info = await event.bot.api.call_action("get_group_member_info", group_id=gid, user_id=married_to)
                name = user_info.get("card") or user_info.get("nickname") or married_to
            except:
                name = f"({married_to})"
            chain.append(Comp.Plain(f"\u200b\n❤已与 {name} 结婚❤"))
        yield event.chain_result(chain)

    @filter.command("搜索")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_search(self, event: AstrMessageEvent, keyword: str | None = None):
        '''搜索角色'''
        event.call_llm = True
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

    @filter.command("添加图片")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_add_image(self, event: AstrMessageEvent, cid: str | int | None = None):
        '''添加图片'''
        event.call_llm = True
        gid = event.get_group_id() or "global"
        user_id = str(event.get_sender_id())
        if cid is None or not str(cid).strip().isdigit():
            yield event.plain_result("用法：添加图片 <角色ID>")
            return
        cid = int(str(cid).strip())
        lock = self._get_group_lock(gid)
        async with lock:
            partners_key = f"{gid}:{user_id}:partners"
            partners = await self.get_kv_data(partners_key, [])
            if str(cid) not in partners:
                yield event.plain_result("角色不在后宫中")
                return
            # Parse image: either from Reply.chain (reply to image) or direct Image in message
            # Only accept Image type (ComponentType.Image), not Video or other segments
            message = event.message_obj.message or []
            images = []
            for part in message:
                if getattr(part, "chain", None):
                    for sub in part.chain:
                        if isinstance(sub, Comp.Image):
                            images.append({"file_name": sub.file, "url": sub.url})
                elif isinstance(part, Comp.Image):
                    images.append({"file_name": part.file, "url": part.url})
            if not images:
                yield event.plain_result("图片在哪呢我请问了")
                return
            custom_images_key = f"{gid}:{cid}:custom_images"
            paths = await self.get_kv_data(custom_images_key, [])
            if len(paths) >= self.custom_images_limit_default:
                yield event.plain_result(f"已经有{self.custom_images_limit_default}张了，别加了")
                return
            if len(paths) + len(images) > self.custom_images_limit_default:
                yield event.plain_result(f"图片太多了")
                return
            logger.info("cid: %s", cid)
            img_dir = f"{self.plugin_data_path.rstrip('/')}/img"
            os.makedirs(img_dir, exist_ok=True)
            async with aiohttp.ClientSession() as session:
                for img in images:
                    short_name = img["file_name"][-15:]
                    save_name = f"{gid}_{cid}_{short_name}"
                    save_path = os.path.join(img_dir, save_name)
                    try:
                        async with session.get(img["url"]) as resp:
                            resp.raise_for_status()
                            data = await resp.read()
                        with open(save_path, "wb") as f:
                            f.write(data)
                        paths.append(f"img/{save_name}")
                        await self.put_kv_data(custom_images_key, paths)
                    except Exception as e:
                        logger.error("add_image save failed: %s", e)
            yield event.chain_result([
                Comp.Reply(id=str(event.message_obj.message_id)),
                Comp.Plain(f"添加成功，当前自定义图片数量：{len(paths)}"),
            ])

    @filter.command("清除图片")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_clear_image(self, event: AstrMessageEvent, cid: str | int | None = None):
        '''清除指定角色的自定义图片'''
        event.call_llm = True
        gid = event.get_group_id() or "global"
        user_id = str(event.get_sender_id())
        if cid is None or not str(cid).strip().isdigit():
            yield event.plain_result("用法：清除图片 <角色ID>")
            return
        cid = int(str(cid).strip())
        married_to = await self.get_kv_data(f"{gid}:{cid}:married_to", None)
        group_role = await self.get_group_role(event)
        if str(married_to) != user_id and group_role not in ['admin', 'owner'] and str(event.get_sender_id()) not in self.super_admins:
            yield event.plain_result("无权限：结婚用户或群管理员可清除该角色自定义图片")
            return
        custom_images_key = f"{gid}:{cid}:custom_images"
        paths = await self.get_kv_data(custom_images_key, [])
        if not paths:
            yield event.plain_result("没有自定义图片")
            return
        for path in paths:
            full_path = os.path.join(self.plugin_data_path, path)
            try:
                os.remove(full_path)
            except:
                pass
        await self.delete_kv_data(custom_images_key)
        yield event.chain_result([
            Comp.Reply(id=str(event.message_obj.message_id)),
            Comp.Plain(f"已清除该角色的自定义图片"),
        ])

    @filter.command("强制离婚")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_force_divorce(self, event: AstrMessageEvent, cid: str | int | None = None):
        '''强制移除指定角色的婚姻，用于清除坏的数据（管理员专用）'''
        event.call_llm = True
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

        # 遍历用户列表检查坏数据
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
        event.call_llm = True
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
        event.call_llm = True
        group_role = await self.get_group_role(event)
        if group_role not in ['admin', 'owner'] and str(event.get_sender_id()) not in self.super_admins:
            yield event.plain_result("无权限执行此命令。")
            return
        config = await self.get_group_cfg(event.get_group_id())
        menu_lines = [
            "用法：",
            "系统设置 抽卡冷却 [0~60]",
            f"———抽卡冷却（秒） | 当前值: {config.get('draw_cooldown', 0)}",
            "系统设置 查询冷却 [0~60]",
            f"———查询冷却（秒） | 当前值: {config.get('query_cooldown', 0)}",
            "系统设置 抽卡次数 [1~10]",
            f"———每小时抽卡次数 | 当前值: {config.get('draw_hourly_limit', self.draw_hourly_limit_default)}",
            "系统设置 后宫上限 [5~50]",
            f"———后宫人数上限 | 当前值: {config.get('harem_max_size', self.harem_max_size_default)}",
            "系统设置 抽卡范围 [>5000]",
            f"———抽卡热度范围 | 当前值: {config.get('draw_scope', '无')}",
            "系统设置 牛头人 [0~100]",
            f"———牛头人概率 | 当前值: {config.get('ntr_chance', 10)}",
            "系统设置 可抽卡时间范围 HH:mm-HH:mm",
            f"———可抽卡时间范围 | 当前值: {config.get('draw_time_range', '无限制')}"
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
            if time > 60:
                yield event.plain_result("时间不能超过60秒")
                return
            config["draw_cooldown"] = time
            await self.put_group_cfg(event.get_group_id(), config)
            yield event.plain_result(f"抽卡冷却已设置为{time}秒")
        elif feature == "查询冷却":
            if value is None or not str(value).strip().isdigit():
                yield event.plain_result("用法：查询冷却 [0~60](秒)")
                return
            time = int(str(value).strip())
            if time < 0:
                time = 0
            if time > 60:
                yield event.plain_result("时间不能超过60秒")
                return
            config["query_cooldown"] = time
            await self.put_group_cfg(event.get_group_id(), config)
            yield event.plain_result(f"查询冷却已设置为{time}秒")
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
                yield event.plain_result("用法：后宫上限 [5~50]")
                return
            count = int(str(value).strip())
            if count < 5:
                count = 5
            if count > 50:
                count = 50
            config["harem_max_size"] = count
            await self.put_group_cfg(event.get_group_id(), config)
            yield event.plain_result(f"后宫上限已设置为{count}")
        elif feature == "抽卡范围":
            if value is None or not str(value).strip().isdigit():
                yield event.plain_result("用法：抽卡范围 [>5000]")
                return
            scope = int(str(value).strip())
            if scope < 5000:
                scope = 5000
            config["draw_scope"] = scope
            await self.put_group_cfg(event.get_group_id(), config)
            yield event.plain_result(f"抽卡范围已设置为热度前{scope}")
        elif feature == "牛头人":
            if value is None or not str(value).strip().isdigit():
                yield event.plain_result("用法：牛头人 [0~100]")
                return
            value = int(str(value).strip())
            if value < 0 or value > 100:
                yield event.plain_result("用法：牛头人 [0~100]")
                return
            config["ntr_chance"] = value
            await self.put_group_cfg(event.get_group_id(), config)
            yield event.plain_result(f"现在抽到别人对象有{value}%概率可牛")
        elif feature == "可抽卡时间范围":
            if value is None:
                yield event.plain_result("用法：可抽卡时间范围 HH:mm-HH:mm")
                return
            time_range_match = re.search(r'^(\d{2}:\d{2})-(\d{2}:\d{2})$', str(value).strip())
            if not time_range_match:
                yield event.plain_result("用法：可抽卡时间范围 HH:mm-HH:mm")
                return
            start_time = time_range_match.group(1)
            end_time = time_range_match.group(2)
            start_hour, start_min = map(int, start_time.split(':'))
            end_hour, end_min = map(int, end_time.split(':'))
            if start_hour < 0 or start_hour > 23 or start_min < 0 or start_min > 59:
                yield event.plain_result("开始时间无效")
                return
            if end_hour < 0 or end_hour > 23 or end_min < 0 or end_min > 59:
                yield event.plain_result("结束时间无效")
                return
            config["draw_time_range"] = f"{start_time}-{end_time}"
            await self.put_group_cfg(event.get_group_id(), config)
            yield event.plain_result(f"可抽卡时间范围已设置为 {start_time} ～ {end_time}")
        else:
            yield event.chain_result([Comp.Plain("\n".join(menu_lines))])

    @filter.command("刷新")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_refresh(self, event: AstrMessageEvent, user_id: str | None = None):
        '''刷新指定用户的抽卡和结婚冷却（群主和超管专用）'''
        event.call_llm = True
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

    @filter.command("群排行")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_group_rank(self, event: AstrMessageEvent):
        '''显示群排名'''
        event.call_llm = True
        gid = event.get_group_id() or "global"
        harem_heats_key = f"{gid}_harem_heats"
        harem_heats = await self.get_kv_data(harem_heats_key, {}) or {}
        if not harem_heats:
            yield event.plain_result("暂无排名数据")
            return
        sorted_heats = sorted(harem_heats.items(), key=lambda x: x[1], reverse=True)[:10]
        rank_lines = []
        for i, (uid, heat) in enumerate(sorted_heats):
            try:
                user_info = await event.bot.api.call_action("get_group_member_info", group_id=gid, user_id=uid)
                name = user_info.get("card") or user_info.get("nickname") or uid
            except:
                name = f"({uid})"
            rank_lines.append(f"{i+1}. {name}：{heat}")
        yield event.plain_result("\n".join(rank_lines))

    @filter.command("终极轮回")
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def handle_ultimate_reset(self, event: AstrMessageEvent, confirm: str | None = None):
        '''清除本群所有角色婚姻信息（除了最爱角色）（群主和超管专用）'''
        event.call_llm = True
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
            harem_heats_key = f"{gid}_harem_heats"
            await self.delete_kv_data(harem_heats_key)
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

