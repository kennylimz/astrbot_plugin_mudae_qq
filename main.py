from typing import Any


from astrbot.api.event import filter, AstrMessageEvent
from astrbot.core.star.filter.platform_adapter_type import PlatformAdapterType
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
import astrbot.api.message_components as Comp
import asyncio
import time
from .util.character_manager import *
import random

DRAW_MSG_TTL = 45  # seconds to keep draw message records
DRAW_MSG_INDEX_MAX = 300  # max tracked message ids to avoid unbounded growth

# 默认值
DRAW_HOURLY_LIMIT = 5  # 每小时抽卡次数
USER_CLAIM_COOLDOWN = 3600  # 结婚冷却（秒）
HAREM_MAX_SIZE = 10  # 后宫上限

@register("二次元笑传之抽抽Bot", "kennylimz", "二次元抽卡插件", "1.0")
class MyPlugin(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        self.super_admins = [] # 超级管理员QQ号列表
        self.group_cfgs = {}
        self.user_lists = {}

    async def initialize(self):
        """可选择实现异步的插件初始化方法，当实例化该插件类之后会自动调用该方法。"""
        load_characters()

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


    # @filter.platform_adapter_type(PlatformAdapterType.AIOCQHTTP)
    # @filter.event_message_type(filter.EventMessageType.PRIVATE_MESSAGE)
    # async def on_private_message(self, event: AstrMessageEvent):
    #     yield event.plain_result("私聊暂不支持！") # 发送一条纯文本消息

    @filter.platform_adapter_type(PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):
        gid = event.get_group_id() or "global"
        uid = event.get_sender_id()
        if uid == event.get_self_id():
            return
        user_set = await self.get_user_list(gid)
        if uid not in user_set:
            user_set.add(uid)
            await self.put_user_list(gid, user_set)

        try:
            post_type = event.message_obj.raw_message.post_type
        except AttributeError:
            post_type = None
        if post_type == "message":
            chain = event.get_messages()
            reply_seg = next((seg for seg in chain if isinstance(seg, Comp.Reply)), None)
            at_segs = [seg for seg in chain if isinstance(seg, Comp.At)]
            at_all = any(isinstance(seg, Comp.AtAll) for seg in chain)
            plain_segs = [seg for seg in chain if isinstance(seg, Comp.Plain)]
            if len(plain_segs)==len(chain):
                # 纯文本消息
                async for result in self.handle_plain_message(event):
                    yield result
                    return
            elif reply_seg:
                # 回复消息
                return
            elif at_all or at_segs:
                # @消息
                async for result in self.handle_at_message(event, at_segs, at_all):
                    yield result
                    return
        elif post_type == "notice":
            notice_type = event.message_obj.raw_message.notice_type
            if notice_type == "group_msg_emoji_like":
                # 群聊表情回应
                async for result in self.handle_emoji_like_notice(event):
                    yield result
                    return
    
    async def handle_reply_message(self, event: AstrMessageEvent, reply_seg):
        return

    async def handle_at_message(self, event: AstrMessageEvent, at_list, at_all: bool):
        # @全体成员，暂不处理
        if at_all:
            return

        self_id = str(event.get_self_id() or "")
        mentioned_self = any(
            str(getattr(seg, "qq", getattr(seg, "target", ""))) == self_id
            for seg in at_list
        )
        if not mentioned_self:
            return

        msg_raw = event.message_str.strip().lower()
        if msg_raw.startswith("菜单") or msg_raw.startswith("帮助"):
            async for result in self.handle_help_menu(event):
                yield result
                return

    async def handle_plain_message(self, event: AstrMessageEvent):
        msg_raw = event.message_str.strip().lower()
        cmd_parts = msg_raw.split()
        gid = event.get_group_id() or "global"

        # 普通指令
        if cmd_parts[0] == "#查询":
            async for res in self.handle_query(event, cmd_parts[1:]):
                yield res
            return
        if cmd_parts[0] == "#搜索":
            async for res in self.handle_search(event, cmd_parts[1:]):
                yield res
            return
        if cmd_parts[0] == "#我的后宫":
            async for res in self.handle_harem(event):
                yield res
            return
        if cmd_parts[0] == "#最爱":
            async for res in self.handle_favorite(event, cmd_parts[1:]):
                yield res
            return
        if cmd_parts[0] == "#离婚":
            async for res in self.handle_divorce(event, cmd_parts[1:]):
                yield res
            return
        if cmd_parts[0] in ["#抽卡", "ck"]:
            async for res in self.handle_draw(event):
                yield res
            return
        if cmd_parts[0] == "#交换":
            async for res in self.handle_exchange(event, cmd_parts[1:]):
                yield res
            return

        # 管理员指令
        if cmd_parts[0] == "#强制离婚":
            group_role = await self.get_group_role(event)
            if group_role not in ['admin', 'owner'] and str(event.get_sender_id()) not in self.super_admins:
                yield event.plain_result("无权限执行此命令。")
                return
            async for res in self.handle_force_divorce(event, cmd_parts[1:]):
                yield res
            return
        if cmd_parts[0] == "#清理后宫":
            group_role = await self.get_group_role(event)
            if group_role not in ['admin', 'owner'] and str(event.get_sender_id()) not in self.super_admins:
                yield event.plain_result("无权限执行此命令。")
                return
            async for res in self.handle_clear_harem(event, cmd_parts[1:]):
                yield res
            return
        if cmd_parts[0] == "#系统设置":
            group_role = await self.get_group_role(event)
            if group_role not in ['admin', 'owner']:
                yield event.plain_result("无权限执行此命令。")
                return
            async for res in self.handle_config(event, cmd_parts[1:]):
                yield res
            return
        
        # 群主/超管指令
        if cmd_parts[0] == "#刷新":
            group_role = await self.get_group_role(event)
            if group_role not in ['owner'] and str(event.get_sender_id()) not in self.super_admins:
                yield event.plain_result("无权限执行此命令。")
                return
            parts = msg_raw.split()
            if len(parts) < 2:
                yield event.plain_result("用法：刷新 <QQ号>")
                return
            user_id = parts[1].strip()
            if not user_id:
                yield event.plain_result("用法：刷新 <QQ号>")
                return
            gid = event.get_group_id() or "global"
            await self.delete_kv_data(f"{gid}:{user_id}:draw_status")
            await self.delete_kv_data(f"{gid}:{user_id}:last_claim")
            yield event.plain_result("次数已重置，结婚冷却已清除")
            return
        if cmd_parts[0] == "#终极轮回":
            group_role = await self.get_group_role(event)
            if group_role not in ['owner'] and str(event.get_sender_id()) not in self.super_admins:
                yield event.plain_result("无权限执行此命令。")
                return
            if len(cmd_parts) != 2:
                yield event.plain_result("用法：终极轮回")
                return
            elif cmd_parts[1] != "确认":
                yield event.plain_result("确定要进行终极轮回吗？此操作将清除本群所有角色婚姻信息（除了最爱角色）。\n如果确定要执行，请使用“终极轮回 确认”")
                return
            await self.reset_all_marriages(event.get_group_id())
            yield event.plain_result("已清除本群所有角色婚姻信息")
            return
        

    async def handle_emoji_like_notice(self, event: AstrMessageEvent):
        notice_type = event.message_obj.raw_message.notice_type
        emoji_user = event.get_sender_id()
        msg_id = event.message_obj.raw_message.message_id
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
                    ts = float(exchange_req.get("ts", 0) or 0)
                    idx_key = f"{gid}:exchange_req_index"
                    idx = await self.get_kv_data(idx_key, [])
                    if not isinstance(idx, list):
                        idx = []
                    if ts and (now_ts - ts > DRAW_MSG_TTL):
                        await self.delete_kv_data(f"{gid}:exchange_req:{msg_id}")
                        new_idx = [item for item in idx if not (isinstance(item, dict) and item.get("id") == msg_id)]
                        if len(new_idx) != len(idx):
                            await self.put_kv_data(idx_key, new_idx)
                        return
                    if str(emoji_user) != str(exchange_req.get("to_uid")):
                        return
                    await self.delete_kv_data(f"{gid}:exchange_req:{msg_id}")
                    new_idx = [item for item in idx if not (isinstance(item, dict) and item.get("id") == msg_id)]
                    if len(new_idx) != len(idx):
                        await self.put_kv_data(idx_key, new_idx)
                    async for res in self.process_swap(event, exchange_req, msg_id):
                        yield res
                    return

    # 功能开关投票
    # 暂未使用
    async def start_toggle_vote(self, event: AstrMessageEvent):
        """发起功能开关投票，记录消息ID以供表情统计。"""
        gid = event.get_group_id() or "global"
        current_state = await self.get_kv_data(f"{gid}:global_toggle", {"enabled": True})
        current_enabled = bool(current_state.get("enabled", True))
        target_action = "关闭" if current_enabled else "开启"
        threshold_text = "一半人同意" if current_enabled else "三分之二人同意"

        msg_prefix = (
            f"当前状态：{'已开启' if current_enabled else '已关闭'}。\n"
            f"这样吧我发起一个投票，{threshold_text}就把功能{target_action}。\n"
            "同意的贴"
        )
        cq_message = [
            {"type": "text", "data": {"text": msg_prefix}},
            {"type": "face", "data": {"id": 76}},
            {"type": "text", "data": {"text": "，不同意的贴"}},
            {"type": "face", "data": {"id": 77}},
            {"type": "text", "data": {"text": "。"}},
        ]

        try:
            resp = await event.bot.api.call_action("send_group_msg", group_id=event.get_group_id(), message=cq_message)
            msg_id = resp.get("message_id") if isinstance(resp, dict) else None
            logger.info({"stage": "toggle_vote_send", "msg_id": msg_id, "resp": resp})
            # 放两个示例表情（不计票）方便操作，后续计算时各减 1
            if msg_id is not None:
                try:
                    await event.bot.api.call_action("set_msg_emoji_like", message_id=msg_id, emoji_id=76, set=True)
                    await event.bot.api.call_action("set_msg_emoji_like", message_id=msg_id, emoji_id=77, set=True)
                except Exception as e:
                    logger.error({"stage": "seed_vote_emoji_error", "error": repr(e), "msg_id": msg_id})

            await asyncio.sleep(120)
            yes = await self.fetch_emoji_count(event.bot, msg_id, "76", "1") if msg_id is not None else 0
            no = await self.fetch_emoji_count(event.bot, msg_id, "77", "1") if msg_id is not None else 0
            yes = max(0, yes - 1)
            no = max(0, no - 1)
            total = yes + no

            if total == 0:
                result_text = "投票未通过：无人在意。"
            else:
                if current_enabled:
                    passed = yes * 2 >= total  # 50% 同意即可关闭
                    need_text = "需50%同意"
                else:
                    passed = yes * 3 >= 2 * total  # >=2/3 同意开启
                    need_text = "需2/3同意"

                if passed:
                    new_enabled = not current_enabled
                    await self.put_kv_data(f"{gid}:global_toggle", {"enabled": new_enabled, "ts": time.time()})
                    result_text = f"投票通过：同意{yes}，反对{no}。功能已{target_action}。"
                else:
                    result_text = f"投票未通过：同意{yes}，反对{no}，{need_text}。功能保持{'开启' if current_enabled else '关闭'}。"
            try:
                await event.bot.api.call_action(
                    "send_group_msg",
                    group_id=gid if gid != "global" else None,
                    message=[{"type": "text", "data": {"text": result_text}}],
                )
            except Exception as e:
                logger.error({"stage": "toggle_vote_result_send_error", "error": repr(e), "msg": result_text})
        except Exception as e:
            logger.error({"stage": "toggle_vote_send_error", "error": repr(e)})
            yield event.plain_result("投票发起失败，请稍后再试。")

    # 获取表情数量
    async def fetch_emoji_count(self, bot, message_id, emoji_id, emoji_type="1") -> int:
        """调用 fetch_emoji_like 获取表情数量，兼容多种返回结构。"""
        try:
            resp = await bot.api.call_action(
                "fetch_emoji_like",
                message_id=message_id,
                emojiId=str(emoji_id),
                emojiType=str(emoji_type),
            )
            logger.info({"stage": "fetch_emoji_like_resp", "emoji_id": emoji_id, "message_id": message_id, "resp": resp})
        except Exception as e:
            logger.error({"stage": "fetch_emoji_like_error", "emoji_id": emoji_id, "error": repr(e)})
            return 0

        likes = []
        try:
            likes = resp["data"]["emojiLikesList"]
        except Exception:
            try:
                likes = resp["emojiLikesList"]
            except Exception:
                likes = []

        bot_id = None
        try:
            bot_id = str(bot.context.self_id)
        except Exception:
            pass

        try:
            if bot_id:
                likes = [x for x in likes if str(x.get("tinyId")) != bot_id]
            return len(likes)
        except Exception:
            return 0

    # 帮助菜单
    async def handle_help_menu(self, event: AstrMessageEvent):
        menu_lines = [
            "@指令：",
            "@我 菜单/帮助",
            "================================",
            "普通指令：",
            "#抽卡/ck",
            "#离婚 <角色ID>",
            "#最爱 <角色ID>",
            "#查询 <角色ID>",
            "#搜索 <角色名称>",
            "#我的后宫",
            "#交换 <我的角色ID> <对方角色ID>",
            "================================",
            "管理员指令：",
            "#系统设置 <功能> <参数>",
            "#清理后宫 <QQ号>",
            "================================",
            "群主/超管指令：",
            "#刷新 <QQ号>",
            "#终极轮回"
        ]
        yield event.chain_result([Comp.Plain("\n".join(menu_lines))])
        return

    # 抽卡逻辑
    async def handle_draw(self, event: AstrMessageEvent):
        user_id = event.get_sender_id()
        gid = event.get_group_id() or "global"
        key = f"{gid}:{user_id}:draw_status"
        now_ts = time.time()
        config = await self.get_group_cfg(gid)
        limit = config.get("draw_hourly_limit", DRAW_HOURLY_LIMIT)
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
        character = get_random_character(limit=config.get('draw_scope', None))
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

        cq_message = [{"type": "text", "data": {"text": f"{name}"}}]
        if image_url:
            cq_message.append({"type": "image", "data": {"file": image_url}})
        if married_to:
            cq_message.append({"type": "text", "data": {"text": "❤已与"}})
            cq_message.append({"type": "at", "data": {"qq": married_to}})
            cq_message.append({"type": "text", "data": {"text": "结婚，勿扰❤"}})
        if remaining == limit-1 and not married_to:
            cq_message.append({"type": "text", "data": {"text": "💡回复任意表情和TA结婚"}})
        if remaining <= 0:
            cq_message.append({"type": "text", "data": {"text": "⚠本小时已达上限⚠"}})

        try:
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
                return
        except Exception as e:
            logger.error({"stage": "draw_send_error_bot", "error": repr(e)})

    # 结婚逻辑
    async def handle_claim(self, event: AstrMessageEvent):
        gid = event.get_group_id() or "global"
        user_id = event.get_sender_id()
        msg_id = event.message_obj.raw_message.message_id
        # per-user cooldown
        config = await self.get_group_cfg(gid)
        cooldown = config.get("claim_cooldown", USER_CLAIM_COOLDOWN)
        now_ts = time.time()
        last_claim_ts = await self.get_kv_data(f"{gid}:{user_id}:last_claim", 0)
        if (now_ts - last_claim_ts) < cooldown:
            wait_sec = int(cooldown - (now_ts - last_claim_ts))
            wait_min = max(1, (wait_sec + 59) // 60)
            yield event.chain_result([
                Comp.At(qq=str(user_id)),
                Comp.Plain(f"结婚冷却中，剩余{wait_min}分钟。")
            ])
            return

        draw_msg = await self.get_kv_data(f"{gid}:draw_msg:{msg_id}", None)
        if not draw_msg:
            return
        ts = draw_msg.get("ts", 0)
        if ts and (now_ts - ts > DRAW_MSG_TTL):
            await self.delete_kv_data(f"{gid}:draw_msg:{msg_id}")
            return
        await self.delete_kv_data(f"{gid}:draw_msg:{msg_id}")
        char_id = draw_msg.get("char_id")
        char = get_character_by_id(char_id)
        if not char:
            return

        # Track per-user marriage list
        marry_list_key = f"{gid}:{user_id}:partners"
        marry_list = await self.get_kv_data(marry_list_key, [])
        if len(marry_list) >= config.get("harem_max_size", HAREM_MAX_SIZE):
            yield event.chain_result([
                Comp.At(qq=user_id),
                Comp.Plain(f" 你的后宫已满{config.get('harem_max_size', HAREM_MAX_SIZE)}，无法再结婚。")
            ])
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

    # 我的后宫
    async def handle_harem(self, event: AstrMessageEvent):
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
            char = get_character_by_id(cid)
            if char is None:
                continue
            heat = char.get("heat") or 0
            total_heat += heat
            fav_mark = ""
            if fav and str(fav) == str(cid):
                fav_mark = "⭐"
            lines.append(f"{fav_mark}{char.get('name')} (ID: {cid})")
        lines.insert(0, f"\u200b\n阵容总人气: {total_heat}")
        chain = [
            Comp.At(qq=event.get_sender_id()),
            Comp.Plain("\n".join(lines))
        ]
        yield event.chain_result(chain)

    # 离婚
    async def handle_divorce(self, event: AstrMessageEvent, cmd_parts: list):
        gid = event.get_group_id() or "global"
        user_id = event.get_sender_id()
        if len(cmd_parts) != 1 or not cmd_parts[0].strip().isdigit():
            yield event.plain_result("用法：离婚 <角色ID>")
            return
        cid = int(cmd_parts[0].strip())
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
        cname = get_character_by_id(cid).get("name") or ""
        yield event.chain_result([
            Comp.Reply(id=cmd_msg_id),
            Comp.At(qq=event.get_sender_id()),
            Comp.Plain(f"已与 {cname or cid} 离婚。"),
        ])

    async def handle_force_divorce(self, event: AstrMessageEvent, cmd_parts: list):
        gid = event.get_group_id() or "global"
        if len(cmd_parts) != 1 or not cmd_parts[0].strip().isdigit():
            yield event.plain_result("用法：强制离婚 <角色ID>")
            return
        cid = int(cmd_parts[0].strip())
        marrried_to = await self.get_kv_data(f"{gid}:{cid}:married_to", None)
        await self.delete_kv_data(f"{gid}:{cid}:married_to")

        user_list = await self.get_user_list(gid)
        for uid in user_list:
            partners_key = f"{gid}:{uid}:partners"
            marry_list = await self.get_kv_data(partners_key, [])
            if str(cid) in marry_list:
                marry_list = [m for m in marry_list if m != str(cid)]
                await self.put_kv_data(partners_key, marry_list)
                fav = await self.get_kv_data(f"{gid}:{marrried_to}:fav", None)
                if fav and str(fav) == str(cid):
                    await self.delete_kv_data(f"{gid}:{marrried_to}:fav")

        cname = (get_character_by_id(cid) or {}).get("name") or cid
        yield event.plain_result(f"{cname} 已被强制解除婚约。")

    # 交换角色
    async def handle_exchange(self, event: AstrMessageEvent, cmd_parts: list):
        gid = event.get_group_id() or "global"
        user_id = event.get_sender_id()
        user_set = await self.get_user_list(gid)
        if len(cmd_parts) != 2 or not cmd_parts[0].strip().isdigit() or not cmd_parts[1].strip().isdigit():
            yield event.plain_result("用法：交换 <我的角色ID> <对方角色ID>")
            return
        my_cid = int(cmd_parts[0].strip())
        other_cid = int(cmd_parts[1].strip())

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
        my_cname = get_character_by_id(my_cid).get("name") or str(my_cid)
        other_cname = get_character_by_id(other_cid).get("name") or str(other_cid)

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

        from_cname = get_character_by_id(from_cid).get("name") or str(from_cid)
        to_cname = get_character_by_id(to_cid).get("name") or str(to_cid)
        yield event.chain_result([
            Comp.Reply(id=str(msg_id)),
            Comp.At(qq=from_uid),
            Comp.Plain(" 与 "),
            Comp.At(qq=to_uid),
            Comp.Plain(f" 已完成交换：{from_cname} ↔ {to_cname}"),
        ])

    # 最爱
    async def handle_favorite(self, event: AstrMessageEvent, cmd_parts: list):
        gid = event.get_group_id() or "global"
        user_id = str(event.get_sender_id())
        if len(cmd_parts) != 1 or not cmd_parts[0].strip().isdigit():
            yield event.plain_result("用法：最爱 <角色ID>")
            return
        cid = cmd_parts[0].strip()
        marry_list_key = f"{gid}:{user_id}:partners"
        marry_list = await self.get_kv_data(marry_list_key, [])
        target = next((m for m in marry_list if str(m) == str(cid)), None)
        if not target:
            yield event.plain_result("你尚未与该角色结婚！")
            return
        cname = get_character_by_id(cid).get("name") or ""
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

    # 查询
    async def handle_query(self, event: AstrMessageEvent, cmd_parts: list):
        if len(cmd_parts) != 1:
            yield event.plain_result("用法：查询 <角色ID>")
            return

        cid = cmd_parts[0].strip()
        if cid.isdigit():
            cid = int(cid)
            char = get_character_by_id(cid)
            if not char:
                yield event.plain_result(f"未找到ID为 {cid} 的角色")
                return
            async for res in self.print_character_info(event, char):
                yield res
                return
        else:
            async for res in self.handle_search(event, [cid]):
                yield res
                return

    # 角色资料卡
    async def print_character_info(self, event: AstrMessageEvent, char: dict):
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
        chain = [Comp.Plain(f"ID: {char.get('id')}\n{name}\n{gender_mark}\nBangumi热度: {heat}")]
        if image_url:
            chain.append(Comp.Image.fromURL(image_url))
        if married_to:
            chain.append(Comp.Plain("❤已与 "))
            chain.append(Comp.At(qq=married_to))
            chain.append(Comp.Plain("结婚❤"))
        yield event.chain_result(chain)

    # 搜索
    async def handle_search(self, event: AstrMessageEvent, cmd_parts: list):
        if len(cmd_parts) != 1:
            yield event.plain_result("用法：搜索 <角色名字/部分名字>")
            return
        keyword = cmd_parts[0].strip()
        matches = search_characters_by_name(keyword)
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

    # 全部角色婚姻重置
    async def reset_all_marriages(self, gid: str):
        """Clear all marriage records for a group by iterating known character IDs."""
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

    # 清理玩家后宫
    async def handle_clear_harem(self, event: AstrMessageEvent, cmd_parts: list):
        gid = event.get_group_id() or "global"
        if len(cmd_parts) != 1 or not cmd_parts[0].strip().isdigit():
            yield event.plain_result("用法：清理后宫 <QQ号>")
            return
        uid = cmd_parts[0].strip()
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

    # 系统设置
    async def handle_config(self, event: AstrMessageEvent, cmd_parts: list):
        config = await self.get_group_cfg(event.get_group_id())
        menu_lines = [
            "用法：",
            f"系统设置 抽卡冷却 [0~600]",
            f"抽卡冷却（秒） | 当前值: {config.get('draw_cooldown', 0)}",
            "系统设置 抽卡次数 [1~10]",
            f"每小时抽卡次数 | 当前值: {config.get('draw_hourly_limit', DRAW_HOURLY_LIMIT)}",
            "系统设置 后宫上限 [5~30]",
            f"后宫人数上限 | 当前值: {config.get('harem_max_size', HAREM_MAX_SIZE)}",
            "系统设置 抽卡范围 [5000~20000]",
            f"抽卡热度范围 | 当前值: {config.get('draw_scope', '无')}",
        ]
        if len(cmd_parts) < 1:
            yield event.chain_result([Comp.Plain("\n".join(menu_lines))])
            return
        feature = cmd_parts[0].strip()
        if feature == "抽卡冷却":
            if len(cmd_parts) != 2 or not cmd_parts[1].strip().isdigit():
                yield event.plain_result("用法：抽卡冷却 [0~600](秒)")
                return
            time = int(cmd_parts[1].strip())
            if time < 0:
                time = 0
            if time > 600:
                yield event.plain_result("时间不能超过600秒")
                return
            config["draw_cooldown"] = time
            await self.put_group_cfg(event.get_group_id(), config)
            yield event.plain_result(f"抽卡冷却已设置为{time}秒")
        elif feature == "抽卡次数":
            if len(cmd_parts) != 2 or not cmd_parts[1].strip().isdigit():
                yield event.plain_result("用法：抽卡次数 [1~10]")
                return
            count = int(cmd_parts[1].strip())
            if count < 1:
                count = 1
            if count > 10:
                yield event.plain_result("次数不能超过10次")
                return
            config["draw_hourly_limit"] = count
            await self.put_group_cfg(event.get_group_id(), config)
            yield event.plain_result(f"每小时抽卡次数已设置为{count}次")
        elif feature == "后宫上限":
            if len(cmd_parts) != 2 or not cmd_parts[1].strip().isdigit():
                yield event.plain_result("用法：后宫上限 [5~30]")
                return
            count = int(cmd_parts[1].strip())
            if count < 5:
                count = 5
            if count > 30:
                count = 30
            config["harem_max_size"] = count
            await self.put_group_cfg(event.get_group_id(), config)
            yield event.plain_result(f"后宫上限已设置为{count}")
        elif feature == "抽卡范围":
            if len(cmd_parts) != 2 or not cmd_parts[1].strip().isdigit():
                yield event.plain_result("用法：抽卡范围 [>3000]")
                return
            scope = int(cmd_parts[1].strip())
            if scope < 5000:
                scope = 5000
            elif scope > 20000:
                scope = 20000
            config["draw_scope"] = scope
            await self.put_group_cfg(event.get_group_id(), config)
            yield event.plain_result(f"抽卡范围已设置为热度前{scope}")
        else:
            yield event.chain_result([Comp.Plain("\n".join(menu_lines))]) 

    async def terminate(self):
        """可选择实现异步的插件销毁方法，当插件被卸载/停用时会调用。"""
