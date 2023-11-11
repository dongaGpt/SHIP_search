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
import requests


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
    tokenizer = BertTokenizer.from_pretrained("/home/donga/projects/SHIP_search/donga_qppp_late_25k_shfpos_t4_absolute_random_bracket_s512_b8/test_tokenizer")
    ctx_encoder = BertModel.from_pretrained("/home/donga/projects/SHIP_search/donga_qppp_late_25k_shfpos_t4_absolute_random_bracket_s512_b8/test_ctx_model")

    

    return 0

if __name__ == '__main__':
    import time
    import gc
    import sys
    korea_timezone = pytz.timezone('Asia/Seoul')

    clean0 = True ## 줄바꾸기를 위한 장치.  clean0=True 일 경우 '\r'을 통해 '마지막 수신' 이 같은 자리에 계속 표시되게 함. clean0이 False로 바뀔 때 \n 을 통해 \r 을 지움.
    while True:
        now0 = datetime.now(korea_timezone)

        if True: #now0.hour > 4 and now0.hour < 5:
            try:
                before_count= mysql_updater(clean0)
                print('shit')

            except NoDataException:
                pass
            except ZeroDataException:
                pass
        else:
            print(f"{now0} : 시간안됨")
        print('end')
        time.sleep(60*60) 

        sys.stdout.flush()
        gc.collect()

