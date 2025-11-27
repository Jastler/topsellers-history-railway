# Steam Top Sellers History Worker

Worker, який проходить по всіх `appid` з Supabase, тягне історичні дані з `games-popularity.com` і зберігає їх у таблицю `steam_topsellers_history`.

## Як працює

- читає `appid` з таблиці `public.steam_app_details`
- для кожного застосунку пагінує API `https://games-popularity.com/swagger/api/game/top-sellers/{appid}`
- пропускає записи новіші за `CUTOFF_TS = 1763734500`
- апсертом заносить (appid, rank, ts) у `analytics.steam_topsellers_history`
- веде прогрес у файлі `progress.json`, щоб можна було продовжити після рестарту

## Налаштування

У коді прописані:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE`

За потреби замініть дані у верхній частині `src/index.js`.

## Запуск

```bash
npm install
npm start
```

Для дев-режиму:

```bash
npm run dev
```

## Файли

- `src/index.js` — основний worker
- `progress.json` — файл прогресу (створиться автоматично)
- `railway.toml` — конфіг для деплою

## Деплой на Railway

1. Залийте код у репозиторій
2. Підключіть репозиторій у Railway
3. Вкажіть змінні середовища (якщо винесете ключі з коду)
4. Запустіть деплой — `npm start`

