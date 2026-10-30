# checks every drafted take and read against its post's pack: coverage, length, bans, numbers that aren't in the pack,
# and a take that repeats the hero number above it. Prints the failures; --merge writes the passing ones into voice.posts
import json,re,glob,sys,os
SP='docs/read/tools/work'  # packs (anuj_N.json, lakme_N.json) and drafts (out_*.json) go here
BAN=re.compile(r'Top ?\d|#\d|—|!|\bnot\b[^.,;:]{0,30}\bbut\b|\bisn.t\b[^.]{0,30}\bit.s\b|\bviews?\b|\bviewers?\b|\breach\b|\bwatch(ed|ing|ers)?\b|\bsaw\b|\bseen\b|scrolled|\binsights?\b|\bmetrics?\b|\bsignals?\b|\bengag\w*|\bperform(ance|ed)\b|\bcontent\b|\bresonat\w*|\bviral\b|\bkiller\b|\bcrush\w*|\bsmash\w*|\bslay\w*|\bbanger\b|\bshould\b|\bneeds to\b',re.I)
NUM=re.compile(r'(?<![\w.])(\d[\d,]*(?:\.\d+)?)(\s?[×xKkM%]|:\d\d)?')
def norm(t): return t.replace(',','')
bad=0; merged={}
for f in sorted(glob.glob(f'{SP}/out_*.json')):
    name=os.path.basename(f)[4:]; pack=json.load(open(f'{SP}/{name}'))
    try: out=json.load(open(f))
    except Exception as e: print('UNREADABLE',f,e); bad+=1; continue
    acct=pack['account']; ids=[p['id'] for p in pack['posts']]; P={p['id']:p for p in pack['posts']}
    miss=[i for i in ids if i not in out]; 
    if miss: print(name,'MISSING',len(miss),miss[:5]); bad+=len(miss)
    for i,v in out.items():
        if i not in P: continue
        src=norm(json.dumps(P[i],ensure_ascii=False)+json.dumps(pack['usual_post'],ensure_ascii=False))
        probs=[]
        tk,rd=v.get('take','').strip(),v.get('read','').strip()
        if not (2<=len(tk.split())<=16): probs.append(f'take words {len(tk.split())}')
        if not (12<=len(rd.split())<=55): probs.append(f'read words {len(rd.split())}')
        for t in (tk,rd):
            m=BAN.search(t)
            if m: probs.append(f'ban "{m.group(0)}"')
            for n,suf in NUM.findall(t):
                tok=norm(n)+(suf or '').strip()
                if norm(n) not in src: probs.append(f'number {n}{suf or ""} not in pack')
        h=P[i].get('hero_number_shown_above_take','').split(' ')[0]
        if h and h in tk: probs.append('take repeats hero')
        if probs: bad+=1; print(f'{name} {i}: {"; ".join(probs)}\n   T: {tk}\n   R: {rd}')
        else: merged.setdefault(acct,{})[i]={'take':tk,'read':rd}
print('failures',bad,'passing',{k:len(v) for k,v in merged.items()})
if '--merge' in sys.argv:
    for acct,posts in merged.items():
        p=f'/Users/sky/feed_me/apps/web/public/read17/{acct}.json'; A=json.load(open(p))
        for i,v in posts.items():
            if i not in A['voice']['posts']: A['voice']['posts'][i]=v
        json.dump(A,open(p,'w'),ensure_ascii=False,separators=(',',':')); print(acct,'posts with a read:',len(A['voice']['posts']))
