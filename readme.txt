### SHIP과 SON으로 나뉨.

검색기는 old와 recent로 나뉨
#old
모델: donga_qppp_all_25k_kykim_shfpos_t4_absolute_random_bracket_s512_b8_gpu5_end40
old는 2019년 이전 데이터. 
동아 계열사 기사 앞 800자만 씀
업데이트 없음.
mysql의  news_gpt.news_old 테이블을 씀

#recent
모델: donga_qppp_late_25k_shfpos_t4_absolute_random_bracket_s512_b8
2019년 이후 데이터
동아 계열사 기사 800자씩 끊어서 모두 다 씀.
업데이트 있음 (하루에 1번 새벽 4시)
mysql의 news_gpt.news_recent 테이블을 씀.


####################################
#######SHIP:  embedding  +  mysql #######
####################################

##SHIP_search  : embedding
recent에 대해 context embedding을 하는 플젝.

#설치 순서는
create_db_and_table.ipynb -> mysql을 만들어줌

build_proto_mysql.ipynb -> 파일을 mysql에 업로드

proto_updater.py -> 파일 이후 시간대를 업데이트

install_main_updater.py  -> 메인 업데이터를 systemctl에 올림
	ㄴ main_updater.py  :  매일 새벽4시 업데이트하고, da_search에 업데이트 사인을 보냄. 그래야 da_search의 index도 업데이트됨


## mysql
news_old와 news_recent가 있음

news_old 
  : 2003~2019 데이터가 있음.
gid를 보내면 기사정보를 회신함.  벡터는 없음

news_recent
  :2019~현재  데이터가 있음
gid를 보내면 기사정보를 회신
벡터도 있음. 벡터는 새로운 index를 만들때 호출함.

####################################
####### SON #######
####################################
da_reader
da_search
da_search_old
da_switch_main


### da_reader
트래픽을 받는 부분.
da_reader.py 가 메인임.

질문 받아서
da_search 혹은 da_search_old  로 보냄.
기사 받으면  opneai에 질문 보내 답을 회신함


###da_search
이 안에 da_switch_main 도 있음

da_search.py 가 메인

## da_switch_main
da_switch_main.py 
http://123123123/update?command=update
받으면 업데이트함.
da_search를 끄고 da_while_switch를 킨 뒤, 업데이트한 뒤 다시 da_while_switc를 끄고 da_search를 키는 시스템임

da_while_switch는 아직 머 제대로 하는게 없음. 아마 오류날듯.
차라리 업데이트중엔 da_search_old로 보내도록 하는게 나을수도. (그렇게 바꿔놓음)

### da_search_old
만들어져있는 index와  pkl 파일을 계속 씀.
