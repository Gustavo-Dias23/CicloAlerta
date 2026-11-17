# Amostras

Um pouco de cada fonte, copiado da pasta `chegada/` depois da 1ª coleta real:

```bash
cp chegada/clima/clima_2026-11-15.json amostras/
head -n 20 chegada/relatos/relatos_2026-11-15.json > amostras/relatos_2026-11-15_amostra.json
python3 -c "import json;d=json.load(open('chegada/ciclovias/'+__import__('os').listdir('chegada/ciclovias')[0]));d['features']=d['features'][:5];json.dump(d,open('amostras/ciclovias_amostra.geojson','w'),ensure_ascii=False)"
```
Os relatos são simulados (ver README principal). Nenhum dado pessoal real.
