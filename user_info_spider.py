import json
import time
import random
import os
import asyncio
from datetime import datetime

from bilibili_api import user, Credential, select_client
from bilibili_api.exceptions import ResponseCodeException
from curl_cffi.requests import AsyncSession

# 强制使用 curl_cffi 引擎增强伪装
try:
    select_client("curl_cffi")
except:
    pass

async def get_new_buvids():
    """获取全新的设备指纹"""
    try:
        async with AsyncSession(impersonate="chrome120") as s:
            resp = await s.get("https://api.bilibili.com/x/frontend/finger/spi", timeout=5)
            data = resp.json().get('data', {})
            return data.get('b_3', ''), data.get('b_4', '')
    except:
        # 兜底方案：随机生成伪指纹
        return f"XY{random.randint(1000, 9999)}{''.join(random.choices('ABCDEF0123456789', k=24))}", ""

class BiliUserInfoSpider:
    def __init__(self, source_file, output_file, cookie_file=None, engine_count=30):
        self.source_file = source_file
        self.output_file = output_file
        self.cookie_file = cookie_file
        self.engine_count = engine_count
        
        self.engines = []
        self.engine_cd = {}  # 记录每个引擎的冷却结束时间戳
        self.ptr = 0         # 轮询指针

    def _load_uids(self):
        """加载待爬取的 UID"""
        if not os.path.exists(self.source_file): return []
        with open(self.source_file, 'r', encoding='utf-8') as f:
            raw = json.load(f)
        all_uids = {int(i['uId']) for i in raw.get('data', {}).get('data', {}).get('danmakus', []) if 'uId' in i}
        
        crawled = set()
        if os.path.exists(self.output_file):
            with open(self.output_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        item = json.loads(line)
                        mid = item.get('mid')
                        if mid: crawled.add(int(mid))
                    except: pass
        return list(all_uids - crawled)

    async def init_engines(self):
        """初始化设备池"""
        print(f"[*] 正在初始化 {self.engine_count} 个用户信息抓取设备...")
        sessdata = bili_jct = None
        if self.cookie_file and os.path.exists(self.cookie_file):
            with open(self.cookie_file, 'r', encoding='utf-8') as f:
                ckpt = json.load(f)
                sessdata = ckpt.get('SESSDATA') or ckpt.get('sessdata')
                bili_jct = ckpt.get('bili_jct')

        for i in range(self.engine_count):
            b3, b4 = await get_new_buvids()
            # 引擎 0 使用登录态，其余使用匿名指纹
            cred = Credential(sessdata=sessdata, bili_jct=bili_jct, buvid3=b3, buvid4=b4) if i == 0 else Credential(buvid3=b3, buvid4=b4)
            name = f"Dev_{i:02d}"
            self.engines.append({"name": name, "cred": cred})
            self.engine_cd[name] = 0
            if i % 10 == 0 and i > 0:
                print(f"    已生成 {i} 个设备...")
            await asyncio.sleep(0.4)
        print(f"[+] 设备池初始化完成.")

    def _get_engine(self):
        """轮询获取未在冷却中的引擎"""
        now = time.time()
        for _ in range(len(self.engines)):
            engine = self.engines[self.ptr]
            self.ptr = (self.ptr + 1) % len(self.engines)
            if self.engine_cd[engine['name']] < now:
                return engine
        return None

    async def fetch(self, uid, engine):
        """执行单次请求 (自动处理 WBI 签名)"""
        try:
            u = user.User(uid=uid, credential=engine["cred"])
            res = await u.get_user_info()
            return "OK", res
        except ResponseCodeException as e:
            return "ERR_API", e.code
        except Exception:
            return "ERR_NET", None

    async def run(self):
        await self.init_engines()
        uids = self._load_uids()
        print(f"[*] 任务开始，共 {len(uids)} 个 UID")

        with open(self.output_file, 'a', encoding='utf-8', buffering=1) as f:
            for i, uid in enumerate(uids):
                while True:
                    engine = self._get_engine()
                    if not engine:
                        await asyncio.sleep(5)
                        continue

                    status, data = await self.fetch(uid, engine)
                    now_ts = datetime.now().strftime("%H:%M:%S")

                    if status == "OK":
                        f.write(json.dumps(data, ensure_ascii=False) + "\n")
                        name = data.get('name', 'N/A')
                        print(f"[{now_ts}][{i+1}/{len(uids)}] {engine['name']} | {uid} | {name}")
                        await asyncio.sleep(random.uniform(0.6, 1.2))
                        break

                    elif status == "ERR_API":
                        # 针对 352/412 错误的处理
                        if data in (-352, -412, 352, 412):
                            self.engine_cd[engine['name']] = time.time() + 60
                            print(f"[{now_ts}] 🚨 {engine['name']} 触发风控({data})，冷冻 1min")
                        elif data == -404:
                            f.write(json.dumps({"mid": uid, "status": "not_found"}) + "\n")
                            break
                        else:
                            self.engine_cd[engine['name']] = time.time() + 60
                    else:
                        self.engine_cd[engine['name']] = time.time() + 10
                        await asyncio.sleep(1)

if __name__ == '__main__':
    spider = BiliUserInfoSpider(
        source_file="data/fanshi_data.json", 
        output_file="data/fanshi_user_info_test.jsonl",
        cookie_file="bili_cookies_B2.json",
        engine_count=30
    )
    asyncio.run(spider.run())