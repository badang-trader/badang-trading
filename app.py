import os, time, math, threading, statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import requests
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)
BASE='https://api.bitget.com'
SESSION=requests.Session()
LOCK=threading.Lock(); NEXT=[0.0]
JOBS={}; JOB_LOCK=threading.Lock()
INTERVALS={'15m':900000,'1H':3600000,'4H':14400000,'1D':86400000}

def api(path, params):
    # Conservative process-wide pacing to avoid API bursts.
    for attempt in range(3):
        with LOCK:
            now=time.monotonic(); delay=max(0,NEXT[0]-now); NEXT[0]=max(now,NEXT[0])+0.14
        if delay: time.sleep(delay)
        try:
            r=SESSION.get(BASE+path,params=params,timeout=12)
            if r.status_code in (429,500,502,503,504):
                time.sleep(0.8*(attempt+1)); continue
            r.raise_for_status(); body=r.json()
            if body.get('code')!='00000': raise RuntimeError(body.get('msg','Bitget API error'))
            return body['data']
        except (requests.RequestException,ValueError) as e:
            if attempt==2: raise RuntimeError(str(e))
            time.sleep(0.8*(attempt+1))
    raise RuntimeError('API retry exhausted')

def ema(vals,period):
    out=[]; k=2/(period+1); v=None
    for x in vals:
        v=x if v is None else x*k+v*(1-k); out.append(v)
    return out

def rsi(vals, period=14):
    if len(vals)<period+2:return None
    diffs=[vals[i]-vals[i-1] for i in range(1,len(vals))]
    gain=sum(max(x,0) for x in diffs[:period])/period
    loss=sum(max(-x,0) for x in diffs[:period])/period
    for d in diffs[period:]:
        gain=(gain*(period-1)+max(d,0))/period
        loss=(loss*(period-1)+max(-d,0))/period
    return 100 if loss==0 else 100-100/(1+gain/loss)

def analyze(symbol,interval,ticker):
    rows=api('/api/v2/mix/market/candles',{'symbol':symbol,'productType':'USDT-FUTURES','granularity':interval,'limit':'180'})
    duration=INTERVALS[interval]; now=int(time.time()*1000)
    candles=sorted(([int(x[0]),float(x[1]),float(x[2]),float(x[3]),float(x[4]),float(x[6])] for x in rows if len(x)>=7),key=lambda x:x[0])
    candles=[c for c in candles if c[0]+duration<=now]
    if len(candles)<65: raise ValueError('완성된 캔들 부족')
    closes=[c[4] for c in candles]; highs=[c[2] for c in candles]; lows=[c[3] for c in candles]; vols=[c[5] for c in candles]
    e20=ema(closes,20); e50=ema(closes,50)
    fast=ema(closes,12); slow=ema(closes,26); macd=[a-b for a,b in zip(fast,slow)]; signal=ema(macd,9); hist=macd[-1]-signal[-1]; prev=macd[-2]-signal[-2]
    strength=rsi(closes); avgvol=statistics.mean(vols[-21:-1]); volratio=vols[-1]/avgvol if avgvol>0 else 0
    trend='상승' if closes[-1]>e20[-1]>e50[-1] else ('하락' if closes[-1]<e20[-1]<e50[-1] else '혼조')
    breakout_up=closes[-1]>max(highs[-21:-1]); breakout_down=closes[-1]<min(lows[-21:-1])
    # Symmetric heuristic; no unverified probability claims.
    long=short=0; reasons_l=[]; reasons_s=[]
    if trend=='상승':long+=30;reasons_l.append('EMA20·50 상승 정렬')
    if trend=='하락':short+=30;reasons_s.append('EMA20·50 하락 정렬')
    if e20[-1]>e20[-4]:long+=10
    if e20[-1]<e20[-4]:short+=10
    if hist>0 and hist>=prev:long+=25;reasons_l.append('MACD 상승 모멘텀')
    if hist<0 and hist<=prev:short+=25;reasons_s.append('MACD 하락 모멘텀')
    if 50<=strength<=70:long+=15;reasons_l.append('RSI 상승 우위')
    if 30<=strength<=50:short+=15;reasons_s.append('RSI 하락 우위')
    if volratio>=1.3:
        if closes[-1]>closes[-2]:long+=10;reasons_l.append('거래량 증가와 양봉')
        if closes[-1]<closes[-2]:short+=10;reasons_s.append('거래량 증가와 음봉')
    if breakout_up:long+=10;reasons_l.append('최근 20봉 고점 돌파')
    if breakout_down:short+=10;reasons_s.append('최근 20봉 저점 이탈')
    direction='관망'; score=max(long,short)
    if long>=65 and long>short:direction='LONG';score=long
    elif short>=65 and short>long:direction='SHORT';score=short
    return {'symbol':symbol,'direction':direction,'score':score,'longScore':long,'shortScore':short,'price':closes[-1],
        'rsi':round(strength,1),'macdHistogram':round(hist,8),'volumeRatio':round(volratio,2),'trend':trend,
        'change24h':round(float(ticker.get('change24h') or 0)*100,2),'quoteVolume24h':float(ticker.get('quoteVolume') or ticker.get('usdtVolume') or 0),
        'fundingRate':float(ticker.get('fundingRate') or 0),'reasons':reasons_l if direction=='LONG' else reasons_s if direction=='SHORT' else [],
        'candleTime':candles[-1][0], 'support':min(lows[-21:-1]),'resistance':max(highs[-21:-1])}

def scan(jobid,interval,minvol):
    def update(**kwargs):
        with JOB_LOCK: JOBS[jobid].update(kwargs)
    try:
        contracts=api('/api/v2/mix/market/contracts',{'productType':'USDT-FUTURES'})
        tickers=api('/api/v2/mix/market/tickers',{'productType':'USDT-FUTURES'})
        allowed={x['symbol'] for x in contracts if x.get('symbolStatus')=='normal' and x.get('symbolType')=='perpetual' and x.get('quoteCoin')=='USDT'}
        candidates=[x for x in tickers if x.get('symbol') in allowed and float(x.get('quoteVolume') or x.get('usdtVolume') or 0)>=minvol]
        candidates.sort(key=lambda x:float(x.get('quoteVolume') or x.get('usdtVolume') or 0),reverse=True)
        update(total=len(candidates),status='running')
        results=[]; errors=0
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures={pool.submit(analyze,x['symbol'],interval,x):x['symbol'] for x in candidates}
            for f in as_completed(futures):
                try: results.append(f.result())
                except Exception: errors+=1
                with JOB_LOCK:
                    JOBS[jobid]['done']+=1
                    JOBS[jobid]['errors']=errors
                    JOBS[jobid]['results']=sorted(results,key=lambda a:(a['direction']=='관망',-a['score']))
        update(status='complete',finishedAt=datetime.now(timezone.utc).isoformat())
    except Exception as e: update(status='error',message=str(e))

@app.get('/')
def index():return render_template('index.html')

@app.post('/api/scan')
def start():
    data=request.get_json(silent=True) or {}; interval=data.get('interval','1H')
    if interval not in INTERVALS:return jsonify(error='잘못된 봉 설정'),400
    try: minvol=float(data.get('minVolume',1000000))
    except (ValueError,TypeError):return jsonify(error='잘못된 거래대금'),400
    if not math.isfinite(minvol) or minvol<0:return jsonify(error='잘못된 거래대금'),400
    with JOB_LOCK:
        active=next((k for k,v in JOBS.items() if v['status'] in ('starting','running')),None)
        if active:return jsonify(jobId=active,alreadyRunning=True)
        JOBS.clear(); jid=str(int(time.time()*1000)); JOBS[jid]={'status':'starting','done':0,'total':0,'errors':0,'results':[],'interval':interval,'startedAt':datetime.now(timezone.utc).isoformat()}
    threading.Thread(target=scan,args=(jid,interval,minvol),daemon=True).start()
    return jsonify(jobId=jid)

@app.get('/api/scan/<jobid>')
def status(jobid):
    with JOB_LOCK:
        job=JOBS.get(jobid)
        return (jsonify(dict(job)) if job else (jsonify(error='검색 작업 없음'),404))

if __name__=='__main__':app.run(host='0.0.0.0',port=int(os.environ.get('PORT','5000')),debug=False)
