# risingwave-playground

[如何使用 RisingWave 和 Supabase 提高电商场景中的广告效果](https://zhuanlan.zhihu.com/p/666189116)

install

```sh
yarn install --immutable
```

## Step 1

```sh
cd docker
docker compose up -d
```

## Setp 2

exec sql `init.sql` and `import.sql`

## Step 3

run sale_events producer

```sh
node index.js
```

## Step 4

listen roi report

```sh
node consumer.js
```
