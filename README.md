# kconnect-jdbc-outbox-transform

Implementação de transforms do [Kafka Connect](https://docs.confluent.io/platform/current/connect) para usar com [io.confluent.connect.jdbc.JdbcSourceConnector](https://www.confluent.io/hub/confluentinc/kafka-connect-jdbc) fazendo queries de uma tabela que implementa [transaction outbox](https://microservices.io/patterns/data/transactional-outbox.html).

## Motivação
---

Podemos usar JdbcSourceConnector e fazer streaming de uma tabela especifica, porém, por default, esse streaming será feito com os dados exatos da tabela (coluna a coluna).

Por exemplo, uma tabela de **user** com colunas: id, name, email, created_at e updated_at, geraria [esse schema avro](docto/schema-user-table-value.avsc). Caso precisássemos alterar a estrutura da tabela, removendo por exemplo alguma coluna, o formato da mensagem mudaria (já que ela reflete nossa tabela), o que poderia quebrar algum consumidor (e ai independente de usar avro ou não, caso um consumidor entendesse um atributo da mensagem como obrigatório e esse atributo deixasse de chegar, ele quebraria).

Usar o [schema registry](https://docs.confluent.io/platform/current/schema-registry/index.html) ajuda a manter a compatibilidade das mensagens, porém esse acoplamento da estrutura da tabela da aplicação com a mensagem notificada, deixa o modelo da aplicação refém para fazer alterações, o que é péssimo para um sistema.

Outro problema é que a estrutura do evento é simples, limitada aos dados da tabela. E se quisessemos saber qual era o valor alterior de um dado? E se quisessemos trabalhar com um evento mais complexo, usando atributos com tipos complexos?

Para isso, ao invés de notificar a tabela, criamos uma estrutura de mensagens de eventos diferente da estrutura das tabelas da aplicação, dai a chamada outbox table. Nela, vamos gravar de forma transacional o evento que queremos enviar, e ao invés de notificar as tabelas da nossa aplicação, passamos a notificar a tabela de outbox. 

Porém a tabela de outbox é como outra tabela qualquer, tem suas colunas, não queremos notificar cada coluna dela, apenas a coluna do evento gravado dentro dela, no mesmo formato que foi gravado. Como fazer então para gravar o evento na coluna e notificar apenas ele com kafka connect? usamos [kafka transforms](https://docs.confluent.io/platform/current/connect/transforms/overview.html). Existem até alguns transforms como o [ExtraField](https://docs.confluent.io/platform/current/connect/transforms/extractfield.html) que permite notificar apenas uma coluna, mas quando usamos avro, como gravar essa coluna? e principalmente, como gravar essa coluna, validar o schema e não ter problemas de compatibilidade caso o schema do avro mude (pq não adianta apenas gravar os bytes do avro, não validar a sua compatibilidade com o schema registry e assim quebrar os consumidores)?

Então assumindo que a mensagem pode ser gravada na tabela de outbox em T1 usando o schema 1. Em T2 ocorre um deploy que atualiza o schema para a versão 2. E então em T3 roda o kafka connect que lê mensagens gravadas com os schemas 1 e 2. 
Nesse processo, queremos algumas garantias:
1) A aplicação não poderia simplesmente gravar os bytes do avro do objeto avro na tabela de outbox, teria que **antes** validar se a mudança feita no schema do avro é aceita, validando a compatibilidade no schema registry (por isso persistimos usando [KafkaAvroSerializer](https://github.com/confluentinc/schema-registry/blob/master/avro-serializer/src/main/java/io/confluent/kafka/serializers/KafkaAvroSerializer.java)). Caso deixassemos gravar sem validar, corremos o risco da aplicação começar a gravar mensagens não compativeis e quando perceber, o trabalho para corrigir é grande.
2) O Kafka Connect tem que conseguir parsear e encaminhar essa nossa mensagem, serializada do jeito que serializamos, uma vez incluida na tabela de outbox, ela **precisa** ser encaminhada, não podemos ter problemas com schema ou parser da mensagem.

Usando debezium, existe um [transform](https://debezium.io/documentation/reference/configuration/outbox-event-router.html) que pode ser usado e que faz isso, mas não queremos usar debezium, queremos usar JdbcSourceConnector. Então criamos um transform que suporta leituras de tabelas de outbox, de eventos serializados usando KafkaAvroSerializer, suportando mudança de versão dos schemas mesmo com delays que podem ocorrer no Kafka connect (enquanto tem evento da versão 1 para enviar, as aplicações já estão gravando a versão 2).

## Formatos suportados

A aplicação grava na tabela de outbox o evento **já serializado no formato do tópico de destino**. O
transform valida o payload nesse formato e o publica sem reserializar: o value do registro são os
bytes gravados (schema `BYTES`), e o connector usa um converter de passagem.

| Formato (`payload.format` ou coluna `table.column.format`) | O que a aplicação grava | Validação antes de publicar |
| --- | --- | --- |
| `avro` | Saída do `KafkaAvroSerializer` | O id do schema existe e é Avro; o payload é lido inteiro com esse schema. Com `avro.use.latest.version=true` (padrão), é reescrito com a versão mais recente de `<tópico>-value` |
| `protobuf` | Saída do `KafkaProtobufSerializer` | O id do schema existe e é Protobuf; os índices da mensagem e a mensagem são lidos com o descriptor |
| `json_schema` | Saída do `KafkaJsonSchemaSerializer` | O id do schema existe e é JSON Schema; o conteúdo é JSON válido (e obedece ao schema com `json.fail.invalid.schema=true`) |
| `json` | JSON em UTF-8 | Exatamente um documento JSON válido |
| `string` (padrão) | Texto | UTF-8 válido |
| `bytes` | Qualquer binário | Nenhuma |

Com `table.column.format`, cada linha informa o próprio formato (linhas com a coluna vazia usam
`payload.format`): uma mesma outbox alimenta tópicos em formatos diferentes.

**Avro e a versão mais recente do schema.** Com `avro.use.latest.version=true`, um evento gravado com
uma versão anterior do schema é convertido pela resolução de schemas do Avro (campos novos recebem o
default) e publicado com o id da versão mais recente do subject. Um evento gravado com uma versão
**mais nova** que a do cache (`schema.cache.ttl`) é publicado como está: o transform nunca rebaixa um
evento nem descarta campos. Com `false`, cada evento sai com o schema com que foi gravado.

Payload nulo publica um tombstone (value nulo), para tópicos compactados.

Cada evento gera uma linha em DEBUG no logger `transform.outbox.JdbcOutbox`:
`Outbox event routed to topic orders.events.json: key=order-2001, format=json, 43 bytes`.

## Compatibilidade e build

| | Versão |
| --- | --- |
| Kafka Connect | 4.x (compilado contra 4.3.1, bytecode Java 17) |
| Cliente do Schema Registry e providers Avro, Protobuf e JSON Schema do Confluent | 8.3.2, empacotados no plugin |
| Build | Gradle 9.5.1 (wrapper) com JDK 25 |

```bash
./gradlew build            # testes + build/distributions/kconnect-jdbc-outbox-smt-<versão>.zip
./gradlew connectPlugin    # build/connect-plugin/kconnect-jdbc-outbox-smt/, pronto para o plugin.path
```

O plugin é um **diretório**, não um jar único: o jar do transform mais as dependências de runtime
(cliente do Schema Registry, Avro, providers de Protobuf e JSON Schema). As bibliotecas do Kafka e o
SLF4J ficam de fora porque o worker já as fornece. Copie o diretório inteiro para o `plugin.path` do
worker (ex.: `/opt/kafka/plugins/kconnect-jdbc-outbox-smt/`).

O value converter do connector não depende do formato:

| Connector | `value.converter` |
| --- | --- |
| JDBC Source | `org.apache.kafka.connect.converters.ByteArrayConverter` |
| Debezium | `io.debezium.converters.BinaryDataConverter` com `value.converter.delegate.converter.type=org.apache.kafka.connect.json.JsonConverter`: os heartbeats do Debezium não são bytes e seguem pelo converter delegado |

Os testes gravam os eventos com os serializers do Confluent, como as aplicações, e leem o que o
transform publica com os deserializers do Confluent, como os consumidores.

Licenças: o transform, o cliente do Schema Registry e o Avro são Apache 2.0. Os providers de Protobuf
e JSON Schema do Confluent (`kafka-protobuf-provider`, `kafka-json-schema-provider`) são Confluent
Community License, que permite uso interno e proíbe oferecer um serviço que concorra com o
Confluent; são os mesmos de que as aplicações já dependem ao usar os serializers Protobuf e JSON
Schema.

## Mudanças na 2.0.0

- **Incompatível**: a classe passou a se chamar `transform.outbox.JdbcOutbox`, e o value do registro são
  os bytes do payload, não mais dados do Connect para o `AvroConverter`. Troque `transforms.<nome>.type`
  e o `value.converter` (tabela acima); o plugin não traz mais o `AvroConverter`. O formato padrão
  passou a ser `string`: connectors com payload Avro declaram `payload.format=avro`.
- Formatos `avro`, `protobuf`, `json_schema`, `json`, `string` e `bytes`, por connector
  (`payload.format`) ou por linha (`table.column.format`), validados antes da publicação.
- `schema.registry.url` só é obrigatório para os formatos com Schema Registry e aceita uma lista
  separada por vírgulas.
- **Correção**: um evento gravado com uma versão anterior do schema é convertido para o schema mais
  recente do tópico por resolução de schemas do Avro (campos novos recebem o default). Antes, qualquer
  campo adicionado ao schema fazia o transform falhar com `ArrayIndexOutOfBoundsException` nos eventos
  antigos ainda na fila.
- **Correção**: um evento gravado com uma versão mais nova que a do cache não é mais rebaixado (antes,
  perdia os campos novos até o cache expirar).
- **Correção**: o schema publicado é o registrado pela aplicação, não um derivado do modelo de dados do
  Connect pelo `AvroConverter`.
- **Correção**: `table.column.partition` aceita qualquer coluna numérica (antes, `INTEGER` gerava
  `ClassCastException`; só `DECIMAL`/`NUMERIC` funcionava).
- `table.column.payload.encode=string` para colunas texto (`text`, `jsonb`); `byte_array` aceita o
  `ByteBuffer` que o Debezium entrega.
- Falhas transitórias do Schema Registry (indisponível, 5xx, 408, 429) viram `RetriableException`:
  com `errors.retry.timeout` no connector, o Kafka Connect repete o registro em vez de parar a task.
- Configuração inválida gera `ConfigException` na criação do connector; o `config()` do transform
  declara todas as opções.
- O cliente do Schema Registry recebe as propriedades padrão do cliente Confluent informadas no
  transform (basic auth, TLS), e os valores de configuração deixaram de ser logados.
- Kafka Connect 4, Confluent 8.3.2, Gradle 9.5.1; bytecode Java 17. Timestamp do registro independente
  do fuso horário da JVM.
- Implementa `Versioned` e publica o manifesto `ServiceLoader` do transform
  (`plugin.discovery=service_load`).

## Como usar 

Essa lib foi usada para usar como exemplo em aula de implementação de outbox, pode conferir [aqui](https://github.com/luizroos/hands-on-microservices/tree/e15), nele ensino a subir o kafka, criar uma imagem do kafka connect com esse transform e executar lendo de uma aplicação de teste.

De qualquer forma, os parâmetros básicos para se usar são esses:

```console
{
  "name": "outbox-connect",
  "config": {
    "name": "outbox-connect",

    "tasks.max": "1",

    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",   // (1) configuramos JdbcSourceConnector
    "mode": "timestamp",
    "timestamp.column.name": "created_at",
    "query": "select message_key, message_payload, message_topic, created_at from outbox_table", // (2) monitoramos a tabela de outbox
    "poll.interval.ms": "1000",
    "batch.max.rows": "5000",

    "connection.url": "jdbc:mysql://mysql:3306/sample-db",
    "connection.user": "db_user",
    "connection.password": "${env:CONNECTOR_SECRET_DB_PASSWORD}",
    "connection.attempts": "5",
    "connection.backoff.ms": "1000",
    "schema.pattern": "sample-db",

    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",  // (3) o transform entrega o payload pronto, em bytes

    "transforms": "outbox",
    "transforms.outbox.type": "transform.outbox.JdbcOutbox",   // (4) declaramos o transform.
    "transforms.outbox.schema.registry.url": "http://schema-registry:8081",  // (5) informamos qual a url do schema registry.
    "transforms.outbox.payload.format": "avro",  // (6) formato do payload gravado (e publicado).
    "transforms.outbox.table.column.payload": "message_payload",  // (7) informamos qual a coluna que tem gravado o payload da mensagem.
    "transforms.outbox.table.column.key": "message_key",  // (8) informamos qual a coluna que grava a chave que será usada para enviar a mensagem.
    "transforms.outbox.table.column.topic": "message_topic"  // (9) informamos qual a coluna que grava o tópico que aquela mensagem pertence
  }
}
```

Descrição de todos os parâmetros:

| Nome                        | Obrigatório | Descrição |
|---------------------------- |:-------------:| -----:|
| schema.registry.url         | para avro, protobuf e json_schema | Endpoints do schema registry, separados por vírgula. |
| schema.cache.ttl            | não           | Tempo em minutos que o schema mais recente de cada tópico fica em cache, default é 60 minutos. Na prática é o tempo máximo para eventos gravados com versões anteriores passarem a sair com uma nova versão do schema Avro |
| payload.format              | não           | Formato do payload: string (default), avro, protobuf, json_schema, json ou bytes |
| table.column.format         | não           | Nome da coluna com o formato de cada linha (mesmos valores de payload.format); vazia usa payload.format |
| avro.use.latest.version     | não           | true (default): publica Avro com a versão mais recente de `<tópico>-value`, sem nunca rebaixar um evento; false: publica com o schema com que o evento foi gravado |
| json.fail.invalid.schema    | não           | true: recusa payloads json_schema que não obedecem ao schema (default false, como nos serializers do Confluent) |
| table.column.payload        | sim           | Nome da coluna que tem os dados da mensagem |
| table.column.payload.encode | não           | Como o payload está na tabela: base64 (valor default), byte_array (coluna binária) ou string (coluna texto, publicada em UTF-8) |
| table.column.key            | sim           | Nome da coluna que tem a chave da mensagem. Não é a PK da tabela, é a chave que será usada como partition key no envio da mensagem |
| table.column.key.encode     | não           | Se a key estiver encodada na tabela, opções possíveis são string (valor default), base64 e byte_array) |
| table.column.topic          | não           | Nome da coluna que tem o nome do tópico que deve ser enviado a mensagem. Apesar de opcional, se não for informado, deve ser informado o parâmetro routing.topic |
| routing.topic               | não           | Nome do tópico que deve ser encaminhado a mensagem, sobrescreve table.column.topic. Use se você tem várias tabelas de outbox ou vai filtrar os eventos de cada tópico via query |
| table.column.headers        | não           | Nome das colunas, separadas por vírgula, para serem adicionadas ao header da mensagem  |
| table.column.partition      | não           | Nome da coluna que mapeia o número da partição que as mensagens devem ser publicadas  |
| propriedades do cliente do Schema Registry | não | Repassadas ao cliente, ex.: `basic.auth.credentials.source=USER_INFO` e `basic.auth.user.info` (use um config provider, nunca o valor literal), `schema.registry.ssl.truststore.location` |

### Com Debezium PostgreSQL (CDC em vez de polling)

O transform só precisa receber a linha da tabela de outbox como um registro plano. Com o Debezium,
aplique antes o `ExtractNewRecordState` e restrinja os dois transforms à tabela de outbox com um
predicate. Os heartbeats do Debezium passam intactos pelos transforms e chegam ao
`BinaryDataConverter` como dados do Connect, que ele entrega ao converter delegado:

```json
{
  "value.converter": "io.debezium.converters.BinaryDataConverter",
  "value.converter.delegate.converter.type": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.delegate.converter.type.schemas.enable": "false",
  "transforms": "unwrap,outbox",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.delete.tombstone.handling.mode": "drop",
  "transforms.unwrap.predicate": "isOutboxTable",
  "transforms.outbox.type": "transform.outbox.JdbcOutbox",
  "transforms.outbox.predicate": "isOutboxTable",
  "transforms.outbox.schema.registry.url": "http://schema-registry:8081",
  "transforms.outbox.table.column.format": "message_format",
  "transforms.outbox.table.column.key": "message_key",
  "transforms.outbox.table.column.payload": "message_payload",
  "transforms.outbox.table.column.payload.encode": "byte_array",
  "transforms.outbox.table.column.topic": "message_topic",
  "predicates": "isOutboxTable",
  "predicates.isOutboxTable.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
  "predicates.isOutboxTable.pattern": "<topic.prefix>\\.public\\.<tabela_outbox>",
  "errors.retry.timeout": "300000"
}
```

### Exemplos

#### table.column.headers

Este parâmetro permite informa um ou mais nomes de colunas que serão inseridas com os respectivos novos no 
header de cada evento processado. Os nomes devem ser separados por vírgula conforme o exemplo a seguir.

```json
{
  "transforms": "outbox",
  "transforms.outbox.table.column.headers": "COLUMNS_1,COLUMNS_2,COLUMNS_3"  
}
```

Se COLUMNS_1 for um Long, COLUMNS_2 for uma String, e COLUMNS_3 um Timestamp, cada um dos valores serão adicionados ao 
header com os tipos de dados lidos do banco.
Quando o parâmetro não é informado, nenhum valor é adicionado ao Header.


#### table.column.partition

Este parâmetro permite informa o nome da coluna que contém o número de partição que o evento deve ser publicado no 
tópico do kafka. Exemplo

```json
{
  "transforms": "outbox",
  "transforms.outbox.table.column.partition": "COLUMNS_PARTITION_NUMBER"  
}
```

A coluna do banco de dados deve ser do tipo numérica e convertida para um Integer (Int32).
Quando o parâmetro não é informado, nenhum número de partição é informado para o kafka.

### table.column.key.encode

Quando a key está codificada (encodada) no banco de dados, é possível decodificada usando decodificação para base64, 
byte_array ou o valor default que é string.

```json
{
  "transforms": "outbox",
  "transforms.outbox.table.column.key.encode": "base64"  
}
```

Quando o parâmetro não é informado, assume-se o valor string, presumindo que o valor para a chave está codificada em 
string.

> **Importante** Se o valor no banco de dados é NULL, informamos uma key randômica (`UUID.randomUUID()`) para evitar 
> qualquer problema com NPE no connector.
