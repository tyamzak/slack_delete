import logging
logging.basicConfig()
import os
import time


from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


slack_token = os.environ.get('SLACK_TOKEN')

client = WebClient(token=slack_token)

TARGET_REACTION = os.environ.get('TARGET_REACTION')
CHA1 = os.environ.get('CHANNEL1')
CHA2 = os.environ.get('CHANNEL2')
CHA3 = os.environ.get('CHANNEL3')
DAYS_AGO = int(os.environ.get('DAYS_AGO'))
channels = [CHA1,CHA2,CHA3]
STARTTIME = time.time()
TIMEOUT = float(os.environ.get('TIMEOUT'))
if not TIMEOUT:
    TIMEOUT = 450
else:
    TIMEOUT = int(TIMEOUT)

count = 0







if not TARGET_REACTION:
    TARGET_REACTION = 'closed_lock_with_key'



def delete_messages_without_reactions(slackmessages:list, channel_id):
    global TIMEOUT
    global STARTTIME
    global TARGET_REACTION
    # response["messages"]が有る場合、１件ずつループ
    for message in slackmessages:

        #スレッドがあればそちらも削除していく
        replies = client.conversations_replies(channel=channel_id,ts = message['ts'])
        #スレッドはRootメッセージが含まれて返ってくる
        if replies['ok'] and len(replies['messages']) > 1:  
            #2つ目以降の要素を削除
            delete_messages_without_reactions(replies["messages"][1:], channel_id)
            
        react = client.reactions_get(channel=channel_id,full = True, timestamp = message['ts'])

        #メッセージにリアクションがつけられているか確認
        if react['ok'] and 'reactions' in react.data['message']:
            # つけられたリアクションがTARGET_REACTIONか確認。
            if TARGET_REACTION in [x['name'] for x in react.data['message']['reactions']]:
                #つけられていれば、次のアイテムへ
                continue

        #Timeout
        if time.time() - STARTTIME > TIMEOUT:
            break
        else:
            time.sleep(1)

        try:
            # 指定したチャットを削除
            client.chat_delete(
                channel=channel_id, ts=message["ts"]
            ) 

        # 引数にチャンネルID、ts（タイムスタンプ：conversations_historyのレスポンスに含まれる）を指定して、削除
        except SlackApiError as e:
            # エラーが発生したら即終了
            return


def hello_pubsub(event, context):
    global TIMEOUT
    global STARTTIME

    for channel_id in channels:
        if channel_id:
            
            try:

                # 450秒でタイムアウトする
                delete_channels(channel_id, DAYS_AGO)
                
                return '時間内に処理が完了しました'
            except StopIteration:
                # Timeoutが発生したとき
                return '時間内に処理が完了ませんでした'
        #Timeout
        if time.time() - STARTTIME > TIMEOUT:
            break
        else:
            pass



def delete_channels(channel_id, DAYS_AGO):
    
    global TIMEOUT
    global STARTTIME
    
    if DAYS_AGO is None:
        TERM = 60 * 60 * 24 * 14 # 秒で表した2週間
    else:
        TERM = 60 * 60 * 24 * DAYS_AGO

    latest = int(time.time() - TERM)  # 現在日時 - 2週間 の UNIX時間
    cursor = None  # シーク位置。最初は None ページを指定して、次からは next_cursor が指し示す位置。

    while True:

        try:
            response = client.conversations_history(  # conversations_history ＝ チャット一覧を得る
                channel=channel_id,
                latest=latest,
                cursor=cursor  # チャンネルID、latest、シーク位置を指定。
                # latestに指定した時間よりも古いメッセージが得られる。latestはUNIX時間で指定する。
            )
        except SlackApiError as e:
            exit

        # response["messages"]が有るか？
        if "messages" in response:  
            delete_messages_without_reactions(response["messages"],channel_id)
                    

        if "has_more" not in response or response["has_more"] is not True:
            # conversations_historyのレスポンスに["has_more"]が無かったり、has_moreの値がFalseだった場合、終了する。
            break
        
        # conversations_historyのレスポンスに["response_metadata"]["next_cursor"]が有る場合、cursorをセット
        if (
            "response_metadata" in response
            and "next_cursor" in response["response_metadata"]
        ):  
            # （上に戻って、もう一度、conversations_history取得）
            cursor = response["response_metadata"]["next_cursor"]
        else:
            break
        
        #Timeout
        if time.time() - STARTTIME > TIMEOUT:
            break
        else:
            time.sleep(1)
