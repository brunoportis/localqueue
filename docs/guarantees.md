# Garantias de entrega

O simpleq é uma fila persistente local baseada em SQLite. A garantia de
entrega é **at-least-once**: uma mensagem pode ser entregue novamente quando a
tentativa anterior não confirmou o processamento.

## O que é confirmado por `put()`

Quando `put()` retorna com sucesso, a mensagem foi inserida em uma transação
SQLite e recebeu um ID. A mensagem permanece no banco até chegar a um estado
terminal (`acked` ou `failed`) ou ser removida por uma operação de manutenção,
como `purge()`.

Com `fsync=False`, a fila usa `synchronous=NORMAL`. Isso protege o estado contra
crashes normais de processo, mas uma falha de energia ou do host pode perder
transações recentes. Para uma política de durabilidade mais forte, use
`fsync=True`, que configura `synchronous=FULL`.

## Ciclo de entrega

```text
ready -> leased -> acked
             |
             +-> ready, quando há NACK ou expiração do lease
             |
             +-> failed, quando o limite de tentativas é atingido
```

Um `get()` cria um lease e um receipt exclusivo para aquela entrega. Se o
worker morrer, não executar `ack()` ou deixar o lease expirar, outro worker
poderá receber a mesma mensagem.

O limite `max_retries` é finito. Portanto, at-least-once não significa que uma
mensagem será tentada indefinidamente: depois do limite, ela vai para
dead-letter (`failed`) e pode ser inspecionada ou reenfileirada explicitamente.

## Por que não é exactly-once

Não existe uma transação única envolvendo a fila e os efeitos externos do
handler. Por exemplo:

```text
worker aplica uma alteração em um serviço externo
    |
    +-- worker morre antes de executar ack()
    |
    +-- o lease expira e a mensagem é entregue novamente
```

O efeito externo pode, portanto, ser executado duas vezes. Os handlers devem
ser idempotentes ou usar uma chave de idempotência no sistema externo, como o
`job_id` da mensagem.

## Fencing token (receipt)

Cada entrega recebe um receipt diferente. Operações de `ack()`, `nack()`,
`fail()` e extensão de lease só são aceitas com o receipt vigente e antes da
expiração do lease.

Isso impede que um worker antigo confirme a entrega atual depois que outro
worker já recebeu a mensagem. O fencing token evita confirmações obsoletas,
mas não elimina duplicidade de efeitos externos.

## Deduplicação com `job_id`

Quando informado, `job_id` é único dentro do nome da fila enquanto o registro
existir. Um novo `put()` com o mesmo `job_id` retorna o ID existente e não
substitui o payload original.

Essa deduplicação evita inserções repetidas, mas não transforma o processamento
em exactly-once. Depois de `purge()`, o registro deixa de existir e o mesmo
`job_id` poderá ser usado novamente.

## O que a fila não promete

- exactly-once para efeitos fora do SQLite;
- ausência de duplicidades durante expiração e recuperação de leases;
- retries infinitos depois de atingir `max_retries`;
- durabilidade contra falha de energia com `fsync=False`;
- retenção indefinida de mensagens que foram removidas por manutenção.
