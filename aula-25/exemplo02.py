import polars as pl 
from datetime import datetime

ENDERECO_DADOS = './../DADOS/PARQUET/NovoBolsaFamilia/'

try:
    print('Inciando o processamento Lazy()')
    inicio = datetime.now()

    # Método Lazy "scan_parquet" cria um plano de execução, não carregando TODOS os dados 
    # diretamente na memória, porém o plano é implementado
    lazy_plan = (
        pl.scan_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')
        .select(['NOME MUNICÍPIO', 'VALOR PARCELA'])
        .group_by('NOME MUNICÍPIO')
        .agg(pl.col('VALOR PARCELA').sum())
        .sort('VALOR PARCELA', descending=True)
    )

    # print(lazy_plan)
    # collect() executa o plano de execução. Ele relamente traz os dados
    df_bolsa_familia = lazy_plan.collect()

    print(df_bolsa_familia.head(10))

    fim = datetime.now()
    print(f'Tempo de execução: {fim - inicio}')
    print('Leitura do arquivo parquet realizada com sucesso!')

except Exception as e:
    print('Erro ao obter os dados')