from __future__ import unicode_literals

import datetime
import errno
import os
import sys
import tempfile
from argparse import ArgumentParser
import numpy as np
import json
#####
from TwitterInformation import GetTweets, GetLatLon, handle_location, flexmessage, GetWeatherInfo
#####

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

from flask import Flask, request, abort, send_from_directory
from werkzeug.middleware.proxy_fix import ProxyFix

from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import (
    LineBotApiError, InvalidSignatureError
)
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage,
    SourceUser, SourceGroup, SourceRoom,
    TemplateSendMessage, ConfirmTemplate, MessageAction,
    ButtonsTemplate, ImageCarouselTemplate, ImageCarouselColumn, URIAction,
    PostbackAction, DatetimePickerAction,
    CameraAction, CameraRollAction, LocationAction,
    CarouselTemplate, CarouselColumn, PostbackEvent,
    StickerMessage, StickerSendMessage, LocationMessage, LocationSendMessage,
    ImageMessage, VideoMessage, AudioMessage, FileMessage,
    UnfollowEvent, FollowEvent, JoinEvent, LeaveEvent, BeaconEvent,
    MemberJoinedEvent, MemberLeftEvent,
    FlexSendMessage, BubbleContainer, ImageComponent, BoxComponent,
    TextComponent, SpacerComponent, IconComponent, ButtonComponent,
    SeparatorComponent, QuickReply, QuickReplyButton,
    ImageSendMessage)

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_host=1, x_proto=1)

cred = credentials.Certificate("xxxxxxxxxxxxxxxxxxxxxxxx")
firebase_admin.initialize_app(cred)
db = firestore.client()

# get channel_secret and channel_access_token from your environment variable
channel_secret = 'xxxxxxxxxxxxxxxxxxx'   
channel_access_token = 'xxxxxxxxxxxxxxxxxxxxxxxxxx'


if channel_secret is None or channel_access_token is None:
    print('Specify LINE_CHANNEL_SECRET and LINE_CHANNEL_ACCESS_TOKEN as environment variables.')
    sys.exit(1)

line_bot_api = LineBotApi(channel_access_token)
handler = WebhookHandler(channel_secret)

static_tmp_path = os.path.join(os.path.dirname(__file__), 'static', 'tmp')




# function for create tmp dir for download content
def make_static_tmp_dir():
    try:
        os.makedirs(static_tmp_path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(static_tmp_path):
            pass
        else:
            raise


@app.route("/callback", methods=['POST'])
def callback():
    # get X-Line-Signature header value
    signature = request.headers['X-Line-Signature']

    # get request body as text
    body = request.get_data(as_text=True)
    app.logger.info("Request body: " + body)

    # chatbot app
    json_line = request.get_json(force=False,cache=False)
    json_line = json.dumps(json_line)

    # handle webhook body
    try:
        #handler.handle(body, signature)
        decoded = json.loads(json_line)
        #print(' dc : ',decoded)
        no_event = len(decoded['events'])

        for i in range(no_event):
            event = decoded['events'][i]
            #print(event)
            event_handle(event)
        
    except LineBotApiError as e:
        print("Got exception from LINE Messaging API: %s\n" % e.message)
        for m in e.error.details:
            print("  %s: %s" % (m.property, m.message))
        print("\n")
    except InvalidSignatureError:
        abort(400)

    return 'OK'


def event_handle(event):
    print(event)
    print('msg : ',event["type"])

    if event["type"] == "follow":
        userId = event['source']['userId']
        print(' Add friend :  from ',userId)
        WriteDataFireStore(event)

    try:
        userId = event['source']['userId']
    except:
        print('error cannot get userId')
        return ''

    try:
        rtoken = event['replyToken']
    except:
        print('error cannot get rtoken')
        return ''

    try:
        msgId = event["message"]["id"]
        msgType = event["message"]["type"]
    except:
        print('error cannot get msgID, and msgType')
        sk_id = np.random.randint(1,17)
        replyObj = StickerSendMessage(package_id=str(1),sticker_id=str(sk_id))
        line_bot_api.reply_message(rtoken, replyObj)
        return ''
       
    if msgType == "text":
        msg = str(event["message"]["text"])
        replyObj = TextSendMessage(text=msg)
        line_bot_api.reply_message(rtoken, replyObj)

    if msgType=="location" :
        lat=event["message"]["latitude"]
        lon=event["message"]["longitude"]
        print(' == >', lat,' :: ',lon)
        print(' Accident Report Part ')
        try:
            df_pd=GetTweets()
            dfAcc_1=GetLatLon(df_pd)
            if(len(dfAcc_1)>0):
                txtresult=handle_location(lat,lon,dfAcc_1,5)
            else:
                txtresult=' ไม่มีเหตุการณ์ '
        except:
            txtresult=' ไม่มีเหตุการณ์ '
        print(' Weather Part ')
        try:
            name, todayStr, status, temp, iconStr, diffStr, rainFlag=GetWeatherInfo(lat,lon)
            replyObj= handle_text(name, todayStr, status,temp,iconStr,diffStr,rainFlag)
            line_bot_api.reply_message(rtoken, replyObj)
        except:
            txtresult=' ERROR - CHECK CODE '
            replyObj=TextSendMessage(text=txtresult)
            line_bot_api.reply_message(rtoken,replyObj)

        line_bot_api.push_message(
                    event["source"]["userId"], TextSendMessage(text=txtresult)
                ) 
    else:
        sk_id = np.random.randint(1,17)
        replyObj = StickerSendMessage(package_id=str(1),sticker_id=str(sk_id))
        line_bot_api.reply_message(rtoken, replyObj)
    return ''

def WriteDataFireStore(event):
    print(event)
    text = event["source"]["userId"]
    #text = 'Befriended'
    output=event["source"]["userId"]
    print(' uid : ',output)
    output1=u'ขอบคุณที่เป็นเพื่อนกันครับ'
    data_ref=db.collection(u'AgentID').document(text)
    data_ref.set({
    u'UserID':output
    }, merge=True)
    line_bot_api.push_message(
            event["source"]["userId"], [
                TextSendMessage(text=output1),
            ]
        )

def handle_text(name, todayStr, status,temp,icon,diffStr,rainFlag):
    flex=flexmessage(name, todayStr, status,temp,icon,diffStr,rainFlag)
    flex=json.loads(flex)
    print(flex)
    replyObj=FlexSendMessage(alt_text=" Rain forecast ", contents=flex)
    return replyObj

@handler.add(MessageEvent, message=TextMessage)
def handle_text_message(event):
    print(event)
    text = event.message.text
    output=event.source.user_id
    output1=u'ขอบคุณที่ลงทะเบียนครับ'
    data_ref=db.collection(u'AgentID').document(text)
    data_ref.set({
    u'UserID':output
    }, merge=True)
    line_bot_api.push_message(
            event.source.user_id, [
                TextSendMessage(text=output1),
            ]
        )
       

@handler.add(FollowEvent)
def handle_follow(event):
    app.logger.info("Got Follow event:" + event.source.user_id)
    line_bot_api.reply_message(
        event.reply_token, TextSendMessage(text=u'กรุณาส่งชื่อ และ นามสกุล กลับมาอีกทีครับ'))


if __name__ == "__main__":
    arg_parser = ArgumentParser(
        usage='Usage: python ' + __file__ + ' [--port <port>] [--help]'
    )
    arg_parser.add_argument('-p', '--port', type=int, default=8000, help='port')
    arg_parser.add_argument('-d', '--debug', default=False, help='debug')
    options = arg_parser.parse_args()

    # create tmp dir for download content
    make_static_tmp_dir()

    app.run(debug=options.debug, port=options.port)


