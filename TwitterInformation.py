### Use with quandl environment
import twint
import nest_asyncio
from datetime import datetime, timedelta
import pytz
import pandas as pd
import numpy as np
from googlemaps import Client as GoogleMaps
import geopy.distance as ps
import warnings
import pyowm
import json
warnings.filterwarnings('ignore')


def GetTweets():
    pd.set_option('display.max_colwidth', 0)

    nest_asyncio.apply()
    #asyncio.set_event_loop(asyncio.new_event_loop())

    todayUTC=datetime.today()

    # dd/mm/YY H:M:S
    # To use code on Heroku need to use UTC time
    #  Use local when testing on local machine
    #to_zone = pytz.timezone('Asia/Bangkok')
    #today=todayUTC.astimezone(to_zone)
    today=todayUTC

    # Get result of the past 30 minutes
    diff=today-timedelta(hours=0, minutes=30)

    todayStr=today.strftime("%Y-%m-%d")+' 00:00:00'
    diffStr=diff.strftime("%Y-%m-%d %H:%M:%S")
    print( ' today : ',todayStr,' -- ',diffStr)
    c=twint.Config()
    c.Hide_output = True
    #c.Username="@js100radio OR from:@fm91trafficpro OR from:@thaich8news OR from:@Ruamduay OR from:@RDNewsCH3"
    c.Limit=15
    #c.Since=todayStr
    c.Since=diffStr
    c.Pandas=True
    c.Search=r"#รถติด OR #อุบัติเหตุ OR #พายุฝน OR #ฝนตก OR #น้ำท่วม"

    twint.run.Search(c)

    #print( ' ===> ',twint.output.panda.Tweets_df)

    def available_columns():
        return twint.output.panda.Tweets_df.columns

    def twint_to_pandas(columns):
        return twint.output.panda.Tweets_df[columns]
    #print(' :: ',available_columns())

    df_pd = twint_to_pandas(["date","timezone","place","username", "tweet", "hashtags"])
    #print(' ---> ',df_pd)
    return df_pd

def GetLatLon(df_pd):
    myAPI='AIzaSyCYA0c5qppFhpcGeWK-e1QIT6EBS3LoMx4'
    gmaps = GoogleMaps(myAPI)  # my account API, replace with yours

    def LatLon_1(dfIn, colName):
        dfIn['lat'] = ""
        dfIn['lon'] = ""

        for x in range(len(dfIn)):
            dummy=dfIn[colName][x]
            #print(dummy)
            geocode_result = gmaps.geocode(dummy)
            #print(geocode_result)
            try:
                dfIn['lat'][x] = geocode_result[0]['geometry']['location']['lat']
                dfIn['lon'][x] = geocode_result[0]['geometry']['location']['lng']
            except:
                dfIn['lat'][x] = ''
                dfIn['lon'][x] = ''
        return dfIn

    dfAcc=LatLon_1(df_pd,'tweet')
    to_zone = pytz.timezone('Asia/Bangkok')
    dateList=[]
    for n in dfAcc['date']:
        stringDate = datetime.strptime(n, '%Y-%m-%d %H:%M:%S')
        tweetTime=stringDate.astimezone(to_zone)
        dateList.append(tweetTime)
        #print(type(n), ' -- ',type(stringDate))


    stringDf=pd.DataFrame(dateList, columns=['datetime'])
    dfAcc=pd.concat([dfAcc,stringDf],axis=1)

    dfAcc['lat'].replace('', np.nan, inplace=True)
    dfAcc['lon'].replace('', np.nan, inplace=True)
    dfAcc_1=dfAcc.dropna().copy().reset_index()
    #print(' ==> ',dfAcc_1)
    return dfAcc_1

def GetDistance(lat, lon, inDf):
    coord1=(float(lat),float(lon))
    latlonLst=inDf[['lat','lon']].values
    #print(type(latlonLst))
    kmLst=[]
    for latlon in latlonLst:
        coord2=(float(latlon[0]),float(latlon[1]))
        kmLst.append(ps.vincenty(coord1,coord2).km)
    inDf['km']=kmLst
    return inDf

def handle_location(lat,lon,inDf, topK):
    result=GetDistance(lat,lon,inDf)
    result=result.sort_values(by='km')
    result=result.iloc[0:topK]
    #print(result)
    resultTxt=''
    for i in range(len(result)):
        kmDistance='%.1f'%(result.iloc[i]['km'])
        time=result.iloc[i]['datetime']
        tweetSource=str(result.iloc[i]['tweet'])
        resultTxt=resultTxt+' ห่าง %s ก.ม.\n เวลา %s \n%s\n\n'%(kmDistance,time,tweetSource)

    print(' :: ',resultTxt)
    return resultTxt[0:-2]

# Flexmessage in Json form built from  https://developers.line.biz/flex-simulator/
def flexmessage(name, todayStr, status,temp,icon,diffStr,rainFlag):

    flex= '''
        {
            "type": "bubble",
            "header": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                {
                    "type": "text",
                    "text": "%s",
                    "color": "#414141",
                    "gravity": "center",
                    "size": "xl",
                    "wrap": true,
                    "flex": 3
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "contents": [
                    {
                        "type": "text",
                        "text": "%s",
                        "size": "xs"
                    },
                    {
                        "type": "text",
                        "text": "%s",
                        "contents": [
                        {
                            "type": "span",
                            "text": "Scatter Cloud"
                        }
                        ]
                    }
                    ]
                },
                {
                    "type": "text",
                    "text": "%s",
                    "color": "#414141",
                    "size": "lg",
                    "align": "end",
                    "gravity": "center",
                    "flex": 1
                },
                {
                    "type": "image",
                    "url": "%s"
                }
                ]
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                    {
                        "type": "text",
                        "text": "Rain Forecast",
                        "wrap": true,
                        "size": "lg",
                        "color": "#a57f23",
                        "gravity": "center"
                    },
                    {
                        "type": "text",
                        "text": "%s"
                    }
                    ],
                    "margin": "xxl"
                },
                {
                    "type": "box",
                    "layout": "baseline",
                    "contents": [
                    {
                        "type": "text",
                        "text": "%s",
                        "color": "#a57f23",
                        "size": "3xl",
                        "align": "center"
                    }
                    ]
                }
                ]
            },"footer": {
                    "type": "box",
                    "layout": "vertical",
                    "contents": [
                    {
                        "type": "text",
                        "text": " "
                    }
                    ]
                },
            "styles": {
                "body": {
                "backgroundColor": "#fdd74b"
                }
            }
            }'''%(name, todayStr, status,str(temp),icon,str(diffStr), str(rainFlag))
    return flex
    



def GetWeatherInfo(lat, lon):
    openWeatherKey="xxxxxxxxxxxxxxxxxxxxxxxxxx"

    todayLocal=datetime.today()

    # dd/mm/YY H:M:S
    to_zone = pytz.timezone('Asia/Bangkok')
    utc = pytz.timezone('UTC')

    today=todayLocal.astimezone(to_zone)
    todayUTC=todayLocal.astimezone(utc)

    #print(today, ' ==== ',todayUTC)
    diff=today+timedelta(hours=2, minutes=0)
    diffUTC=todayUTC+timedelta(hours=2, minutes=0)

    todayStr=today.strftime("%Y-%m-%d %H:%M:%S")
    diffStr=diff.strftime("%Y-%m-%d %H:%M:%S")

    # create client
    owm = pyowm.OWM(openWeatherKey)
    obs = owm.weather_at_coords(lat, lon) 
    w = obs.get_weather()

    # Weather details
    wind= w.get_wind()                  # {'speed': 4.6, 'deg': 330}
    humid=w.get_humidity()              # 87
    temp=w.get_temperature('celsius')  # {'temp_max': 10.5, 'temp': 9.7, 'temp_min': 9.0}
    rain=w.get_rain()  
    status=w.get_detailed_status()
    icon=w.get_weather_icon_url() 
    l = obs.get_location()
    name=l.get_name()
    #print(name, ' :: ',wind,' :: ',humid,' :: ',temp,' :: ',rain,' :: ',status,' :: ',icon)  

    fc = owm.three_hours_forecast(name)
    f = fc.get_forecast()
    rainFlag=fc.will_be_rainy_at(diffUTC)

    #print(' AT ---- ', diff,' ==> ',rainFlag)
    
    iconStr='https'+icon[4:]
    iconStr

    print(name, str(todayStr), status,str(temp["temp"]),iconStr, str(diffStr), str(rainFlag) )
    return name, todayStr, status, temp["temp"],iconStr, diffStr, rainFlag



