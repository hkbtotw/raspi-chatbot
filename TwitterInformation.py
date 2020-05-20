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
import requests
warnings.filterwarnings('ignore')


def GetTweets():
    pd.set_option('display.max_colwidth', 0)

    nest_asyncio.apply()

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
    myAPI='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
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
def flexmessage(name, todayStr, status,temp,icon,diffStr,rainFlag, flagDesc):

    iconDict={
        1:"https://openweathermap.org/img/w/01d.png",
        2:"https://openweathermap.org/img/w/02d.png",
        3:"https://openweathermap.org/img/w/03d.png",
        4:"https://openweathermap.org/img/w/04d.png",
        5:"https://openweathermap.org/img/w/09d.png",
        6:"https://openweathermap.org/img/w/10d.png",
        7:"https://openweathermap.org/img/w/10d.png",
        8:"https://openweathermap.org/img/w/11d.png",
        9:"https://openweathermap.org/img/w/13d.png",
        10:"https://openweathermap.org/img/w/13d.png",
        11:"https://openweathermap.org/img/w/13d.png",
        12:"https://openweathermap.org/img/w/01d.png"
    }

    #if(rainFlag==True):
    if(rainFlag==5 or rainFlag==6 or rainFlag==7 or rainFlag==8):
        textRain=flagDesc
        colorRain="#FF0000"
        bgColor="#FFA07A"
        iconRain=iconDict[rainFlag]
    else:
        textRain=flagDesc
        colorRain="#228B22"
        bgColor="#98FB98"
        iconRain=iconDict[rainFlag]

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
                        "size": "xs",
                        "color": "#a57f23",
                        "gravity": "center"
                    },
                    {
                        "type": "text",
                        "text": "%s",
                        "wrap": true,
                        "size": "xs",
                        "color": "#a57f23",
                        "gravity": "center",
                        "align":"end"
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
                        "color": "%s",
                        "size": "xs",
                        "align": "center"
                    },
                    {
                        "type": "icon",
                        "url": "%s",
                        "size": "xl",
                        "position": "relative",
                        "margin": "none"
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
                "backgroundColor": "%s"
                }
            }
            }'''%(name, todayStr, status,str(temp),icon,str(diffStr), textRain,colorRain, iconRain,bgColor)
    return flex
    



def GetWeatherInfo(lat, lon):
    openWeatherKey="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

    todayLocal=datetime.today()

    # dd/mm/YY H:M:S
    to_zone = pytz.timezone('Asia/Bangkok')
    utc = pytz.timezone('UTC')

    today=todayLocal.astimezone(to_zone)
    todayUTC=todayLocal.astimezone(utc)

    #print(today, ' ==== ',todayUTC)
    diff=today+timedelta(hours=3, minutes=0)
    diffUTC=todayUTC+timedelta(hours=3, minutes=0)

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
    print(name, ' :: ',wind,' :: ',humid,' :: ',temp,' :: ',rain,' :: ',status,' :: ',icon)  
    logDateStr='None'
    try:
        fc = owm.three_hours_forecast(name)
        #f = fc.get_forecast()

        logDate=fc.when_starts('date')
        logDateStr=logDate.astimezone(to_zone).strftime("%Y-%m-%d %H:%M:%S")
        rainFlag=fc.will_have_rain()
        
        #rainFlag=fc.will_be_rainy_at(diffUTC)
    except:
        rainFlag="No prediction"

    #print(' AT ---- ', diff,' ==> ',rainFlag)
    print(' AT ---- ', logDateStr,' ==> ',rainFlag)
    
    iconStr='https'+icon[4:]
    iconStr
    print(name, str(todayStr), status,str(temp["temp"]),iconStr, logDateStr, str(rainFlag) )
    return name, todayStr, status, temp["temp"],iconStr, logDateStr, rainFlag




def GetForecast(lat,lon):
    condDict={
        1:'ท้องฟ้าแจ่มใส (Clear)',
        2 : 'มีเมฆบางส่วน (Partly cloudy)',
        3 : 'เมฆเป็นส่วนมาก (Cloudy)',
        4 : 'มีเมฆมาก (Overcast)',
        5 : 'ฝนตกเล็กน้อย (Light rain)',
        6 : 'ฝนปานกลาง (Moderate rain)',
        7 : 'ฝนตกหนัก (Heavy rain)',
        8 : 'ฝนฟ้าคะนอง (Thunderstorm)',
        9 : 'อากาศหนาวจัด (Very cold)',
        10 : 'อากาศหนาว (Cold)',
        11 : 'อากาศเย็น (Cool)',
        12 : 'อากาศร้อนจัด (Very hot)'
        }

    mode='tc,cond'
    todayLocal=datetime.today()

    # dd/mm/YY H:M:S
    to_zone = pytz.timezone('Asia/Bangkok')
    utc = pytz.timezone('UTC')

    today=todayLocal.astimezone(to_zone)
    todayUTC=todayLocal.astimezone(utc)

    diff=today+timedelta(hours=3, minutes=0)
    diffUTC=todayUTC+timedelta(hours=3, minutes=0)

    todayStr=today.strftime("%Y-%m-%d")
    hourStr=today.strftime("%H")
    minStr=today.strftime("%M")

    date=todayStr

    #print(' --> ',todayStr,' ** ',hourStr)
    if(int(minStr)<=30):
        durationStr='2'
    else:
        durationStr='3'
    #durationStr='2'

    # Use URL from opendata website
    url = 'https://data.tmd.go.th/nwpapi/v1/forecast/location/hourly/at'  

    stringSearch='?lat=%s&lon=%s&fields=%s&date=%s&hour=%s&duration=%s'%(lat,lon,mode,date,hourStr,durationStr)

    url=url+stringSearch
    print(url)

    # Use your API key
    headers = {'accept': 'application/json','authorization': 'Bearer '+'xxxxxxxxxxxxxxxxxxxxxxxxxxxx'}

    response = requests.get(url, headers=headers)

    result=response.json()
    #print(result)
    fc=result['WeatherForecasts'][0]
    #print(fc)

    #data=pd.DataFrame.from_dict(result['WeatherForecasts'])
    forecast=pd.DataFrame.from_dict(fc['forecasts'])
    
    
    def GetHour(dt):
        return dt.strftime("%H")
    
    forecast['datetime']=pd.to_datetime(forecast['time'], format="%Y-%m-%dT%H:%M:%S%z")
    forecast['hour']=forecast.apply(lambda x: GetHour(x['datetime']),axis=1)
    #print(forecast)
    
    current=forecast.iloc[0]
    nexthour=forecast.iloc[len(forecast)-1]


    currentInformation=current['data']
    forecastInformation=nexthour['data']
    forecastTime=nexthour['datetime'].strftime("%Y-%m-%d %H:%M:%S")

    forecastCond=condDict[forecastInformation['cond']]

    
    return forecastInformation['cond'], forecastCond,forecastTime



