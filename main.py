import logging
logging.basicConfig(level=logging.DEBUG)

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

slack_token = ''
channel_id = ''
client = WebClient(token=slack_token)

count = 0

def delete_messages_without_reactions(slackmessages:list):
    
    # response["messages"]が有る場合、１件ずつループ
    for message in slackmessages:

        #スレッドがあればそちらも削除していく
        replies = client.conversations_replies(channel=channel_id,ts = message['ts'])
        #スレッドはRootメッセージが含まれて返ってくる
        if replies['ok'] and len(replies['messages']) > 1:  
            #2つ目以降の要素を削除
            delete_messages_without_reactions(replies["messages"][1:])
            
        time.sleep(1)
        react = client.reactions_get(channel=channel_id,full = True, timestamp = message['ts'])

        #メッセージにリアクションがつけられているか確認
        if react['ok'] and 'reactions' in react.data['message']:
            # つけられたリアクションがTARGET_REACTIONか確認。
            if TARGET_REACTION in [x['name'] for x in react.data['message']['reactions']]:
                #つけられていれば、次のアイテムへ
                continue

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
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
                        event. The `@type` field maps to
                         `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
                        The `data` field maps to the PubsubMessage data
                        in a base64-encoded string. The `attributes` field maps
                        to the PubsubMessage attributes if any is present.
         context (google.cloud.functions.Context): Metadata of triggering event
                        including `event_id` which maps to the PubsubMessage
                        messageId, `timestamp` which maps to the PubsubMessage
                        publishTime, `event_type` which maps to
                        `google.pubsub.topic.publish`, and `resource` which is
                        a dictionary that describes the service API endpoint
                        pubsub.googleapis.com, the triggering topic's name, and
                        the triggering event type
                        `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
    Returns:
        None. The output is written to Cloud Logging.
    """
    import base64

    print("""This Function was triggered by messageId {} published at {} to {}
    """.format(context.event_id, context.timestamp, context.resource["name"]))

    if 'data' in event:
        name = base64.b64decode(event['data']).decode('utf-8')
    else:
        name = 'World'
    print('Hello {}!'.format(name))

import time
TARGET_REACTION = 'closed_lock_with_key'
TERM = 60 * 60 * 24 * 7 * 2 # 秒で表した2週間
TERM = 0
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
        delete_messages_without_reactions(response["messages"])
                

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
    time.sleep(1)
