#encoding:utf-8
'''
Created on 2016年7月25日

@author: wilson.zhou
'''
import urllib
import re
def func(url):

    if url==None:
        return None
    else:
        url_decode = urllib.unquote(url)
        print(url_decode)
        start=url_decode.find("://")
        if start!=-1:
            end=url_decode.find("/",start+len("://"))
            if end!=-1:
                host=url_decode[start+len("://"):end]
            else:
                host=url_decode[start+len("://"):]
        
            if host.startswith("[") and host.endswith("]"):
                return host
            subNames=host.split(".")
            numPieces=len(subNames)
            if numPieces<=2:
                return host
            if numPieces==4  and re.match("[0-9]{1,}\.[0-9]{1,}\.[0-9]{1,}\.[0-9]{1,}",str(host)):
                return host
            firstHostPiece = 0
            ccTLDs ="ac ad ae af ag ai al am an ao aq ar as at au aw ax az ba bb bd be bf bg bh bi " + \
        "bj bl bm bn bo br bs bt bv bw by bz ca cc cd cf cg ch ci ck cl cm cn co cr cu " + \
        "cv cx cy cz de dj dk dm do dz ec ee eg eh er es et eu fi fj fk fm fo fr ga gb " + \
        "gd ge gf gg gh gi gl gm gn gp gq gr gs gt gu gw gy hk hm hn hr ht hu id ie il " + \
        "im in io iq ir is it je jm jo jp ke kg kh ki km kn kp kr kw ky kz la lb lc li " + \
        "lk lr ls lt lu lv ly ma mc md me mf mg mh mk ml mm mn mo mp mq mr ms mt mu mv " + \
        "mw mx my mz na nc ne nf ng ni nl no np nr nu nz om pa pe pf pg ph pk pl pm pn " + \
        "pr ps pt pw py qa re ro rs ru rw sa sb sc sd se sg sh si sj sk sl sm sn so sr " + \
        "st su sv sy sz tc td tf tg th tj tk tl tm tn to tp tr tt tv tw tz ua ug uk um " + \
        "us uy uz va vc ve vg vi vn vu wf ws ye yt yu za zm zw"
            ccNeverTLDs="bd bn bt ck cr cy do eg er et fj fk gh gn gt gu jm ke kh kw lb lr ml mm mt mv " + \
        "my mz ni np om pa pe pg pw py qa sa sb sv sy th tr tz uy ve ye yu zm zw"
            ccAlwaysTLDs="am ao aq as ax bf bg bh bi bj bw by bz cc cd cf cg ch ci cl cm cv cx cz de dj " + \
        "dk eu fi fm fo ga gd gf gl gm gq gs gw gy hm ie io km kn la li lt lu md mh mp " + \
        "mq mr ms mu na nc ne ng nl nu sh si sk sl sm sn sr st su sz tc td tf tg tk tl " + \
        "tm tn to tv va vc vg vu ws"
            gTLDs="aero arpa asia biz cat com coop edu gov info int jobs mil mobi museum name net " + \
        "org pro tel"
            if ccTLDs.find(subNames[-1].lower())>=0:
                
                if  ccNeverTLDs.find(subNames[-1].lower())>=0:
                    firstHostPiece=numPieces-3
                elif ccAlwaysTLDs.find(subNames[-1].lower())>=0:
                    firstHostPiece = numPieces - 2
                elif len(subNames[numPieces-2])<=2:
                    firstHostPiece = numPieces - 3
                elif gTLDs.find(subNames[numPieces - 2].lower())>=0:
                    firstHostPiece = numPieces -3
                else:
                    firstHostPiece = numPieces-2
            elif gTLDs.find(subNames[-1].lower())>=0:
                if ccTLDs.find(subNames[numPieces - 2].lower())>=0:
                    firstHostPiece = numPieces -3
                else:
                    firstHostPiece = numPieces -2
            else:
                firstHostPiece = firstHostPiece
          
            if firstHostPiece==0:
                return host
            else:
                str1=""
                for i in xrange(firstHostPiece,numPieces):
                    str1=str1+subNames[i]
                    str1=str1+"."
                return str1[:-1]

        else:
            return None
print(func('http://wangyou.pcgames.com.cn/319/3190181.html'))
    
  