import os
import glob
import json
import pandas as pd
import numpy as np
import re
import warnings

# ================= 配置区 =================
# 在这里填入你想要跳过的主播前缀
EXCLUDE_LIST = [] 
# ==========================================

warnings.filterwarnings('ignore')

def extract_features_for_streamer(base_name):
    """提取单个主播的特征并保存为 CSV"""
    file_info = f'{base_name}_user_info_raw.jsonl'
    file_card = f'{base_name}_user_card_raw.jsonl'
    file_live = f'{base_name}_data.json'
    
    if not all(os.path.exists(f) for f in [file_info, file_card, file_live]):
        print(f"⚠️  [{base_name}] 原始文件不全，跳过...")
        return None

    print(f"🔄 正在处理主播: [{base_name}] ...")
    
    # --- A. 处理 JSONL (User Info) ---
    dict_a = {}
    with open(file_info, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                item = json.loads(line.strip())
                mid = item.get('mid')
                if not mid: continue
                # 跳过注销号/报错号
                if item.get('status') == 'deleted/not_found' or 'original_code' in item:
                    continue
                
                dict_a[mid] = {
                    'mid': mid, 
                    'level': item.get('level', 0),
                    'is_default_face': 1 if 'noface.jpg' in str(item.get('face', '')) else 0,
                    'is_nft_face': item.get('face_nft', 0),
                    'is_annual_vip': 1 if item.get('vip', {}).get('type', 0) == 2 else 0,
                    'is_senior': item.get('is_senior_member', 0),
                    'has_nameplate': 1 if item.get('nameplate', {}).get('nid', 0) > 0 else 0,
                    'has_pendant': 1 if item.get('pendant', {}).get('pid', 0) > 0 else 0,
                    'has_medal': 1 if item.get('fans_medal', {}).get('wear', False) else 0,
                    'has_sign': 1 if item.get('sign') else 0
                }
            except: continue

    # --- B. 处理 JSONL (User Card) ---
    dict_b = {}
    with open(file_card, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                item = json.loads(line.strip())
                card = item.get('card', {})
                mid = int(card.get('mid', 0)) if card else int(item.get('mid', 0))
                if not mid: continue
                if item.get('status') == 'deleted/not_found' or not card:
                    continue
                
                name = str(card.get('name', ''))
                dict_b[mid] = {
                    'mid': mid,
                    'is_default_name': 1 if re.match(r'^bili_\d+$', name) else 0,
                    'pub_videos': item.get('archive_count', 0),
                    'fans': card.get('fans', 0)
                }
            except: continue

    # --- C. 处理标准 JSON (Live Data) ---
    dict_c = {}
    with open(file_live, 'r', encoding='utf-8') as f:
        try:
            raw = json.load(f)
            events = raw.get('data', {}).get('data', {}).get('danmakus', [])
            stats = {}
            for e in events:
                uid = e.get('uId'); t = e.get('type')
                if not uid: continue
                if uid not in stats:
                    stats[uid] = {'msgs':[], 'pay_flag':0}
                
                if t == 0: # 弹幕
                    stats[uid]['msgs'].append(str(e.get('message', '')))
                elif t in [1, 2, 3]: # 礼物、上舰、SC
                    stats[uid]['pay_flag'] = 1
            
            for uid, s in stats.items():
                m = s['msgs']
                dict_c[uid] = {
                    'mid': uid, 
                    'msg_cnt': len(m),
                    'msg_unique': len(set(m))/len(m) if len(m) > 0 else 1.0,
                    'avg_msg_len': np.mean([len(x) for x in m]) if len(m) > 0 else 0,
                    'is_paying_user': s['pay_flag']
                }
        except: pass

    # --- 合并 ---
    df_c = pd.DataFrame.from_dict(dict_c, orient='index')
    if df_c.empty: return None
    df_a = pd.DataFrame.from_dict(dict_a, orient='index')
    df_b = pd.DataFrame.from_dict(dict_b, orient='index')
    
    df = pd.merge(df_c, df_a, on='mid', how='left')
    df = pd.merge(df, df_b, on='mid', how='left')
    df['Group'] = base_name.split("data\\")[-1]
    df = df.fillna(0)
    
    # 保存该主播个体特征为 CSV
    df.to_csv(f'{base_name}_features.csv', index=False, encoding='utf-8-sig')
    return df

def run_analysis():
    target_files = glob.glob('data/*_data.json')
    streamers = [f.replace('_data.json', '') for f in target_files]
    
    if not streamers:
        print("❌ 文件夹内未找到数据文件。")
        return

    all_streamers_dfs = []
    for s in streamers:
        if s in EXCLUDE_LIST: continue
        res = extract_features_for_streamer(s)
        if res is not None: all_streamers_dfs.append(res)
    
    if not all_streamers_dfs: return

    print("\n📊 正在生成最终汇总报表...")
    summary_rows = []
    
    for df in all_streamers_dfs:
        name = df['Group'].iloc[0]
        total = len(df)
        
        # 1. 账号底蕴
        lv_high = (df['level'] >= 5).sum()
        lv_low = (df['level'] <= 2).sum()
        senior_cnt = (df['is_senior'] == 1).sum()
        annual_vip = (df['is_annual_vip'] == 1).sum()
        
        # 2. 账号外观/静态
        default_name = (df['is_default_name'] == 1).sum()
        face_default = (df['is_default_face'] == 1).sum()
        no_sign = (df['has_sign'] == 0).sum()
        white_cnt = (df['pub_videos'] == 0).sum()
        zero_fans_cnt = (df['fans'] == 0).sum()
        
        # 3. 装扮与成就
        nameplate_cnt = (df['has_nameplate'] == 1).sum()
        pendant_cnt = (df['has_pendant'] == 1).sum()
        medal_cnt = (df['has_medal'] == 1).sum()
        nft_cnt = (df['is_nft_face'] == 1).sum()
        
        # 4. 行为
        # 高度复读机：发言 >= 2 条 且 唯一率 < 0.6
        repeater_cnt = ((df['msg_cnt'] >= 2) & (df['msg_unique'] < 0.6)).sum()
        paying_cnt = (df['is_paying_user'] == 1).sum()
        
        # 过滤0发言后计算平均弹幕长度
        real_talkers = df[df['msg_cnt'] > 0]
        avg_danmaku_len = real_talkers['avg_msg_len'].mean() if not real_talkers.empty else 0
        
        # 按照用户给出的表头顺序组织数据
        row = {
            '主播组别': name,
            '总交互人数': total,
            '高级号(>=5级)%': round(lv_high / total * 100, 2),
            '低级号(<=2级)%': round(lv_low / total * 100, 2),
            '硬核会员%': round(senior_cnt / total * 100, 2),
            '年度大会员%': round(annual_vip / total * 100, 2),
            '默认昵称(bili_数字)%': round(default_name / total * 100, 2),
            '默认头像%': round(face_default / total * 100, 2),
            '无个人签名%': round(no_sign / total * 100, 2),
            '0投稿数号%': round(white_cnt / total * 100, 2),
            '0粉丝号%': round(zero_fans_cnt / total * 100, 2),
            '拥有成就勋章%': round(nameplate_cnt / total * 100, 2),
            '佩戴头像框%': round(pendant_cnt / total * 100, 2),
            '佩戴粉丝牌%': round(medal_cnt / total * 100, 2),
            'NFT头像%': round(nft_cnt / total * 100, 2),
            '高度复读机(>=2条且去重<0.6)%': round(repeater_cnt / total * 100, 2),
            '人均弹幕量(条)': round(df['msg_cnt'].mean(), 2),
            '平均弹幕长度(字)': round(avg_danmaku_len, 2),
            '付费用户比例%': round(paying_cnt / total * 100, 2)
        }
        summary_rows.append(row)
    
    final_report = pd.DataFrame(summary_rows)
    final_report = final_report.sort_values('总交互人数', ascending=False)
    
    # 导出 CSV
    report_name = '直播间观众特征分析报告.csv'
    final_report.to_csv(report_name, index=False, encoding='utf-8-sig')
    
    print(f"✅ 任务完成！报告已保存至: {report_name}")

if __name__ == "__main__":
    run_analysis()