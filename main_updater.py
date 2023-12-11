####
# v1 : db 0 업데이트 작업
# 10초에 한번 api로 받아와, doc2vec 에 넣은 뒤  벡터 산출해 저장.
# api 정보는 mysql 에도 동시에 저장.

#v2 : online_checker 반영.
import requests
import json
import mysql.connector
from database import config
from datetime import date, timedelta, datetime
from transformers import BertTokenizer, BertModel
import binascii
import re
import hanja
import numpy as np
from normalize_text import normalize_regex_kykim
import torch
import pytz
import logging


####### transformer #######

# tokenizer = BertTokenizer.from_pretrained("/home/donga/projects/SHIP_search/donga_qppp_late_25k_shfpos_t4_absolute_random_bracket_s512_b8/test_tokenizer")
# ctx_encoder = BertModel.from_pretrained("/home/donga/projects/SHIP_search/donga_qppp_late_25k_shfpos_t4_absolute_random_bracket_s512_b8/test_ctx_model")
#################################

class NoDataException(Exception):
    pass
class ZeroDataException(Exception):
    pass
def online_check(title0):
    tit0 = title0.replace(' ','')
    online_list = ['영감한스푼','양종구의100세시대건강법','이헌재의인생홈런','베스트닥터의베스트건강법','병을이겨내는사람들','허진석의톡톡스타트업','전승훈의아트로드']
    for obj in online_list:
        if obj in tit0:
            return True
    return False

def mysql_updater(clean0):
    print('업데이트 시작')
    # 업데이트 할때만 메모리에 올라가도록
    tokenizer = BertTokenizer.from_pretrained("/home/donga/projects/SHIP_search/donga_qppp_late_25k_shfpos_t4_absolute_random_bracket_s512_b8/test_tokenizer")
    ctx_encoder = BertModel.from_pretrained("/home/donga/projects/SHIP_search/donga_qppp_late_25k_shfpos_t4_absolute_random_bracket_s512_b8/test_ctx_model")

    today0 = date.today()

    articles = []
    api_dic = {}
    for n in range(5,-1,-1):  # 3,2,1,0 ## 5,-1,-1
        day0 = today0 - timedelta(days=n)  # 어제(n=1), 오늘(n=0)
        print(day0)
        url = f'https://openapi.donga.com/newsList?p={day0.strftime("%Y%m%d")}'
        temp = requests.get(url)
        temp = json.loads(temp.content)
        if 'data' not in [*temp]:
            # 그냥 가끔 data 없이 올 떄가 있음. 그리고 새벽에 첫 기사 나오기 전까지는 'data'가 없음.
            print('### NoDataException ###')
            raise NoDataException()
        articles += temp['data']

    if len(articles) ==0:
        print('### ZeroDataException ###')
        raise ZeroDataException

    ## 1) 새로 업데이트된 기사 정보를 mysql에 쌓음. 또 본문을 word2vec 으로 vec 변환해 redis에  'gid : vec' 형태로 저장함.

    db = mysql.connector.connect(**config)
    cursor = db.cursor()

    # 작업을 수행하기 전 상태 체크
    cursor.execute("""
    select gid from news_gpt.news_recent
    """)
    already = np.array(cursor.fetchall()).reshape(1,-1)[0]
    before_count = len(already)
    num_recieve = len(articles)  # api로 받은 숫자

    already = list(map(lambda x:x[:-2], already)) # '_0' 떼기
    already = list(set(already)) #중복 제거
    num_original = len(already) # 쪼개기 전 숫자

    num_deal = 0  # 실제 처리대상 숫자.. 
    num_doc2vec = 0  # doc2vec 처리돼 redis와 mysql에 들어간 숫자.
    write_ars = [] # 등록할 기사 목록
    del_compare = {}  # del_gid 와 비교할 대상.  api에 기사가 있는지 체크하기 위한 목적
    for ar in articles:
        gid_ori = ar['gid']
        content = ar["content"]
        if ar['source'] in ['동아일보', '동아닷컴', '신동아', '주간동아', '여성동아', '스포츠동아'] and len(content) > 500 :
            del_compare[gid_ori] = ar
            if gid_ori not in already:  ## api에서 받은 게 mysql 에 없다면 (gid_origin 비교하는거임)
                

            # if ar['source'] in ['동아일보','동아닷컴','스포츠동아','경제뉴스','어린이동아','어린이동아','게임동아','여성동아','신동아','주간동아','에듀동아','뉴스1','뉴시스(웹)'] and len(content) > 500:
            
                title = ar['title']#.replace('"', '“')

                if (online_check(title) and ar['ispublish'] == '1') or (
                        '서영아의100세카페' in title.replace(' ', '') and ar['ispublish'] == '0'):
                    pass
                else:
                    write_ars.append(ar)  # 작성대상 리스트에 쌓음.

    if len(write_ars) > 0:
        # 모델 : ctx_model
        for ar in write_ars:

            #################################################
            
            title_ori = ar['title']#.replace('"', '“') 한자변환 안한 제목

            content = re.sub(r'\.com$','', ar['content'].strip())
            content = normalize_regex_kykim(content).replace('  ',' ')
            title = ar['title']
            title = normalize_regex_kykim(title).replace('  ',' ')
            if re.search(r'[^ㄱ-ㅎ가-힣a-zA-Z0-9…·##μ●▲▶\'!$%&()*+,./:;<=>?@\^`{|}~∼％"\[\]\s-]', content):
                content = hanja.translate(content,'substitution')
            if re.search(r'[^ㄱ-ㅎ가-힣a-zA-Z0-9…·##μ●▲▶\'!$%&()*+,./:;<=>?@\^`{|}~∼％"\[\]\s-]', title):
                title = hanja.translate(title,'substitution')
            

            createtime = datetime.strptime(ar['createtime'], '%Y-%m-%d %H:%M:%S') - timedelta(hours=5)

            #쪼개기
            count0 = len(content)//800 #조각 수
            if count0 ==0:
                count0=1   
                # 800자 미만인건 그거만.
                # 800자 이상인건, 다음 블록 중 800자 이상인거만.

            for i in range(count0):
                num_deal += 1
                num_doc2vec +=1

                api_dic[f"{ar['gid']}_{i}"] ={'title': title_ori, 'thumburl':ar['thumburl']}

                con = content[i*800 :(i+1)*800 + 15]
                if i!=0:
                    con = '…' + re.sub('^[^\s]+\s','',con) # 앞에 자르고
                ctx = f"<제목> {title} <작성일> {createtime.year}년 {createtime.month}월 {createtime.day}일 <본문> {con}"[:805] 
                ctx = re.sub('\s[^\.\s]*$','',ctx) #뒤에 자르고
                ctx += '…'

                print(f"기사 등록 : {ar['gid']}_{i} / {title_ori} / {ar['url']}")


                ## embedding
                with torch.no_grad():
                    a = tokenizer(ctx, max_length=512, padding='max_length',truncation=True, return_tensors='pt')
                    vec = ctx_encoder(**a)[0][:,0,:]
                vec = np.array(vec)
                vec = np.reshape(vec,(-1,))  # (1,768) 을 (768,) 로 만들어줌.  dtype= float32
                #########################

                # mysql에 넣기
                cursor.execute(f"""insert ignore into news_gpt.news_recent values(
                    "{ar['gid']}_{i}",
                    "{ar['createtime']}",
                    %s,
                    %s,
                    %s,
                    "{ar['url']}",
                    "{ar['thumburl']}",
                    "{ar['source']}",
                    "{ar['cate_code']}",
                    %s
                    )""", (title_ori.replace(' ',''), title_ori, ctx.encode('utf-8'), vec.tobytes()))
                    # 벡터를 mysql에도 저장. 인출떄는  np.array(json.loads(cursor.fetchall()[0][0]))
                ###################################################################
        db.commit()

    # 2) 기사 수정 및 삭제
    db = mysql.connector.connect(**config)
    cursor = db.cursor()
    cursor.execute(
        f"""
        select gid, title, createtime, thumburl from news_gpt.news_recent where createtime >="{today0 - timedelta(days=1)}" and createtime < "{today0 + timedelta(days=1)}" 
        """
    )
    mysql_dic = {g: (t, c, th) for g, t, c, th in cursor.fetchall()}  # gid : title

    ### mysql 에 있는 것 중 api 에 없는것을 찾아야 함
    del_gid = []
    # correct_dic = {}  # gid : 바꿀제목
    correct_title = {}
    correct_thumburl = {}
    for gid in mysql_dic:
        # print(mysql_dic[gid][1], end=' ')
        gid_ori = gid.split('_')[0]
        if gid_ori not in del_compare:  ## mysql에 있는 gid가 api에는 없는경우  ==> 삭제
            #삭제할거
            if clean0:
                print('\n============')
                clean0 = False
            del_gid.append(gid)
            print(f"삭제 : {gid} / {mysql_dic[gid][0]}/ {mysql_dic[gid][1]}")

        elif del_compare[gid_ori]['title'] != mysql_dic[gid][0]:  # gid가 있긴 한데 title이 다를 경우 ==> 수정
            if clean0:
                print('\n============')
                clean0 = False
            print(f"제목 수정 : {gid} / {mysql_dic[gid][0]} => {del_compare[gid_ori]['title']}  ")
            correct_title[gid] = del_compare[gid_ori]['title']

        elif del_compare[gid_ori]['thumburl'] != mysql_dic[gid][2]:  # gid가 있긴 한데 title이 다를 경우 ==> 수정
            if clean0:
                print('\n============')
                clean0 = False
            print(f"썸네일url 수정 : {gid} / {mysql_dic[gid][2]} => {del_compare[gid_ori]['thumburl']}  ")
            correct_thumburl[gid] = del_compare[gid_ori]['thumburl']
            
        else:
            # print("이상없음")
            pass

    num_deleted = len(del_gid) # 삭제 수
    num_corrected = len(*[correct_title]) + len(*[correct_thumburl]) # 수정 수
    ## 삭제  실행
    if len(del_gid) >0:
        print(del_gid)

        cursor.execute(
            f"""
                delete from news_gpt.news_recent where gid in ({','.join(del_gid)});
                """
        )


    ## 수정 실행
    sql = """update news_gpt.news_recent set title=%s where gid=%s"""
    cursor.executemany(sql, [(value, key) for key, value in correct_title.items()])
    db.commit()

    sql = """update news_gpt.news_recent set thumburl=%s where gid=%s"""
    cursor.executemany(sql, [(value, key) for key, value in correct_thumburl.items()])
    db.commit()

    ## 활동 정리
    cursor.execute("""
    select count(*), max(createtime), min(createtime) from news_gpt.news_recent
    """)
    after_count, after_max, after_min = cursor.fetchone()

    return before_count, after_count, after_max, after_min, num_recieve, num_deal, num_doc2vec, num_deleted, num_corrected, clean0, num_original

if __name__ == '__main__':
    import time
    import gc
    # import sys
    last= datetime(2000,1,1)
    korea_timezone = pytz.timezone('Asia/Seoul')

    clean0 = True ## 줄바꾸기를 위한 장치.  clean0=True 일 경우 '\r'을 통해 '마지막 수신' 이 같은 자리에 계속 표시되게 함. clean0이 False로 바뀔 때 \n 을 통해 \r 을 지움.
    print("시작")
    while True:
        now0 = datetime.now(korea_timezone)

        if now0.hour == 4  and now0.day != last.day: 
        # if now0.hour == 19:
            try:
                before_count, after_count, after_max, after_min, num_recieve, num_deal, num_doc2vec, num_deleted, num_corrected, clean0, num_original = mysql_updater(clean0)
                if num_doc2vec !=0 or num_deleted != 0 or num_corrected !=0: #처리한게 하나라도 있으면.
                    if clean0:
                        print('\n============')
                        clean0 = False
                    print(f"마지막 업데이트 : {now0} // mysql : {before_count} -> {after_count} (split 전 : {num_original}) // api {num_recieve}개 받아 {num_doc2vec}개 전환// {num_deleted}개 삭제 {num_corrected}개 수정// {after_min} ~ {after_max}")
                    print("============")
                    
                    last = now0
                else:  # 처리한 게 하나도 없을 경우.
                    clean0 =True
                    # print(f"마지막 수신 : {now0}",end='||')
                    print(f"데이터가 없음 (?)")


                # 검색기 업데이트
                update_result0 = requests.get('http://10.0.203.159:5100/update?command=update')
                results0 = json.loads(update_result0.content)
                print(f"검색기 업데이트 결과 : {results0}")

            except NoDataException:
                pass
            except ZeroDataException:
                pass
        else:
            print(f"{now0} : 시간안됨")
        time.sleep(60*30) 

        # sys.stdout.flush()  # 이거때메 로그 안남는듯
        gc.collect()

