####### 실제 동아일보 서비스에 올릴 retriever 콜퍼스 만들기 #######
### 여러 버전 시도
#1)['동아일보', '동아닷컴', '신동아', '주간동아', '여성동아', '스포츠동아']  이거만 남김    corpus_late_onlydonga
#2)['동아일보', '동아닷컴', '신동아', '주간동아', '여성동아', '스포츠동아']  이거만 남기고 800자 단위로 split 함   corpus_late_onlydonga_split


### time 다 붙어있는 retrival unit 만들기 ####
import numpy as np
import jsonlines
## 전체 gid list 
all_gid_list = np.load("/convei_nas2/intern/ybseo/self_DPR/yb_search/datas/donga/data/gid_list.npy")

## 전체 데이터
with jsonlines.open("/convei_nas2/intern/ybseo/self_DPR/yb_search/datas/donga/crawl_data/from20030101_filtered.jsonl", 'r') as reader:
    all_data = list(reader)
class0 = 'retrieve_unit'
class_gid_list = np.load(f"/convei_nas2/intern/ybseo/self_DPR/yb_search/datas/donga/train_val_test_index/{class0}_gid.npy")
is_index = np.isin(all_gid_list, class_gid_list) # [True, False ,,,,]

n=0


from tqdm import tqdm
from normalize_text import normalize_regex_kykim
import hanja
import re
from datetime import datetime, timedelta
from copy import deepcopy
n=0
type0 = 'corpus_old'  # corpus_late, corpus_old
with jsonlines.open(f"/convei_nas2/intern/ybseo/self_DPR/yb_search/3_build_faiss_search/datas/donga/{type0}_onlydonga_split.jsonl", 'w') as writer:
    
    for i, val in tqdm(enumerate(is_index)):

        if val:
            dic = deepcopy(all_data[i])

            createtime = dic['createtime']
            
            createtime = datetime.strptime(createtime, '%Y-%m-%d %H:%M:%S') - timedelta(hours=5)
            if (type0 == 'corpus_late' and createtime.year >= 2019) or (type0 == 'corpus_old' and createtime.year < 2019):
                if dic['source'] in ['동아일보', '동아닷컴', '신동아', '주간동아', '여성동아', '스포츠동아'] :
                    
                    content = dic['content']
                    content = normalize_regex_kykim(content).replace('  ',' ')
                    title = dic['title']
                    title = normalize_regex_kykim(title).replace('  ',' ')
                    if re.search(r'[^ㄱ-ㅎ가-힣a-zA-Z0-9…·##μ●▲▶\'!$%&()*+,./:;<=>?@\^`{|}~∼％"\[\]\s-]', content):
                        content = hanja.translate(content,'substitution')
                    if re.search(r'[^ㄱ-ㅎ가-힣a-zA-Z0-9…·##μ●▲▶\'!$%&()*+,./:;<=>?@\^`{|}~∼％"\[\]\s-]', title):
                        title = hanja.translate(title,'substitution')

                    gid = dic['gid']
                    del dic['gid']
                    del dic['content']
                    del dic['len']
                    del dic['date_gid']

                    #쪼개기
                    count0 = len(content)//800 #조각 수
                    if count0 ==0:
                        count0=1   
                        # 800자 미만인건 그거만.
                        # 800자 이상인건, 다음 블록 중 800자 이상인거만.
                    for i in range(count0):
                        con = content[i*800 :(i+1)*800 + 15]
                        if i!=0:
                            con = '…' + re.sub('^[^\s]+\s','',con) # 앞에 자르고
                        ctx = f"<제목> {title} <작성일> {createtime.year}년 {createtime.month}월 {createtime.day}일 <본문> {con}"[:805] 
                        ctx = re.sub('\s[^\.\s]*$','',ctx) #뒤에 자르고
                        ctx += '…'

                        dic['ctx'] = ctx

                        dic0 = {f"{gid}_{i}": dic}

                        # temp = all_data[i]
                        # temp[f'id_{class0}'] = n  # 각자의 아이디 입력
                        writer.write(dic0)
                        n+=1

print(n)


## corpus_old_onlydonga_split  :  1289500
## corpus_old_onlydonga  :  1030317
## corpus_late_onlydonga_split  :  350561
## corpus_late_onlydonga  :  258605  # 'corpus_for_service_jsonl_to_tsv.py' 에서 만듦.
1030317