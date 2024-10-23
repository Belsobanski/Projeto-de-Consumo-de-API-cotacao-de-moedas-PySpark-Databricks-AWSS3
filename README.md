# Projeto de Consumo de API de Cotação de Moedas com PySpark, Databricks e AWS S3

## Descrição

Este projeto tem como objetivo consumir uma API de cotação de moedas em tempo real, processar os dados utilizando **PySpark** no **Databricks**, e armazenar os dados na **AWS S3** em duas camadas de dados: **raw** e **gold**.

# API Exemplo

### Queremos pegar a cotação do dólar de forma automática e atualizada, como fazemos ?

Usamos o awesome api: https://docs.awesomeapi.com.br/

API do Twilio comentada: https://www.twilio.com/docs/libraries/python

![image](https://github.com/user-attachments/assets/188e9f1e-0ed3-43db-8509-4f968f7f9fa9)


## Tecnologias Utilizadas

- **PySpark**: Processamento e transformação de dados.
- **Databricks**: Plataforma para desenvolvimento e execução dos scripts PySpark.
- **API de Cotação de Moedas**: Fonte de dados para capturar as cotações em tempo real.
- **AWS S3**: Armazenamento dos dados em camadas **raw** e **gold**.

## Estrutura do Projeto

1. **Camada Raw**: Armazena os dados da API na forma bruta, sem modificações.
2. **Camada Gold**: Após o processamento, os dados são transformados e limpos, prontos para análises e consumo.

## Funcionalidades

- Consumo de dados da API em tempo real.
- Pipeline de processamento de dados automatizado com PySpark.
- Armazenamento organizado em diferentes camadas de dados no S3.
- Execução do pipeline no Databricks.

## Como Executar

1. **Pré-requisitos**:
   - Conta na **AWS** com permissões para armazenar dados no **S3**.
   - Acesso ao **Databricks** para execução do código PySpark.
   - Bibliotecas necessárias: PySpark, boto3.

2. **Configurações**:
   - Atualize as credenciais da AWS para garantir que o PySpark consiga acessar e armazenar os dados no S3.
  
     
