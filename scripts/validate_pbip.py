"""Static structural checks only. Does NOT replace opening Power BI Desktop."""
from pathlib import Path
import json
import re


def main():
    root = Path(__file__).resolve().parents[1] / 'powerbi'
    report = root/'ChicagoTaxi.Report/definition'
    model = root/'ChicagoTaxi.SemanticModel/definition'
    page_list = json.loads((report/'pages/pages.json').read_text(encoding='utf-8'))['pageOrder']
    assert len(page_list) == 4 and len(set(page_list)) == 4, page_list
    tables = {}
    for file in (model/'tables').glob('*.tmdl'):
        txt = file.read_text(encoding='utf-8')
        tables[file.stem] = set(re.findall(r'^\tcolumn ([^\n]+)',txt,re.M)) | set(re.findall(r"^\tmeasure '(.*?)' =",txt,re.M))
        if '\t\tsource = ' in txt and 'Table.Transform' in txt:
            expr = txt.split('\t\tsource = ',1)[1]
            expr = re.sub(r'"(?:[^"]|"")*"','""',expr)
            stack=[]
            for ch in expr:
                if ch in '([{':stack.append(ch)
                elif ch in ')]}':
                    assert stack and {'(':')','[':']','{':'}'}[stack.pop()]==ch, file
            assert not stack, file
    assert len(tables)==7,sorted(tables)
    visuals = list((report/'pages').rglob('visual.json'))
    ids = []
    for f in visuals:
        data=json.loads(f.read_text(encoding='utf-8'))
        assert data['name']==f.parent.name,f
        ids.append(data['name'])
        pos=data['position']
        assert 0<=pos['x'] and 0<=pos['y'] and pos['x']+pos['width']<=1280 and pos['y']+pos['height']<=720,f
        for q in data['visual'].get('query',{}).get('queryState',{}).values():
            for p in q.get('projections',[]):
                field=p['field']; shape=field[next(iter(field))]
                table=shape['Expression']['SourceRef']['Entity'];col=shape['Property']
                assert table in tables and col in tables[table],(f,table,col)
    assert len(ids)==len(set(ids)), 'visual ids are not unique'
    for id_ in page_list:
        p=json.loads((report/'pages'/id_/'page.json').read_text(encoding='utf-8'))
        assert p['name']==id_,id_
    for f in root.rglob('*.json'):
        json.loads(f.read_text(encoding='utf-8'))
    for f in [root/'ChicagoTaxi.pbip', report.parent/'definition.pbir',model.parent/'definition.pbism']:
        json.loads(f.read_text(encoding='utf-8'))
    assert len(visuals)>=45
    print(f'PASS: {len(page_list)} PBIP pages, {len(visuals)} native visuals, {len(tables)} model tables, valid JSON, references and balanced M syntax')
    print('LIMITATION: Windows Power BI Desktop open/refresh still requires validation on user machine')


if __name__=='__main__':
    main()
