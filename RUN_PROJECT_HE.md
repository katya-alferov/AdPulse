<div dir="rtl">

# 🚀 מדריך הרצה מעשי – AdPulse

המדריך הזה מסביר צעד-אחר-צעד איך להריץ את כל המערכת על המחשב שלך, איך לבדוק שכל חלק עובד, ומה לעשות כשמשהו נתקע.
כל הפקודות נבדקו בפועל. הפלטים ה"צפויים" למטה הם פלטים אמיתיים מהרצה (המספרים אצלך יהיו שונים, כי הנתונים אקראיים).

> 💡 **כלל זהב:** הכול רץ אצלך במחשב. אין ענן, אין תשלום, אין כרטיס אשראי.

---

## 0. מה צריך לפני שמתחילים

| דרישה | מינימום | הערה |
|---|---|---|
| זיכרון RAM | 8GB (מומלץ 16GB) | Spark ו-Kafka אוהבים זיכרון |
| מקום בדיסק | 6GB פנויים | האימג'ים של Docker תופסים כ-3.5GB |
| Docker | Docker Desktop (Windows/Mac) או Docker Engine (Linux) | ראו סעיף 1 |
| Git | כל גרסה עדכנית | כדי להוריד את הקוד |
| Python 3.11+ | לא חובה | רק אם רוצים להריץ בדיקות או מצב דמו מחוץ ל-Docker |

### האם Docker Desktop חינמי בשבילך? (נבדק)
- **Docker Desktop** חינמי לשימוש **אישי**, ללימודים, לפרויקטי קוד פתוח לא מסחריים, ולעסקים קטנים (פחות מ-250 עובדים **וגם** פחות מ-10 מיליון דולר הכנסות בשנה). פרויקט לימודי אישי כמו שלך – חינמי.
- **Docker Engine** (בלינוקס) – קוד פתוח וחינמי לגמרי, לכל שימוש.
- אם אי פעם תצטרכי חלופה חינמית לגמרי גם בעבודה: **Rancher Desktop** (Apache-2.0) או **Colima** במק (MIT).

---

## 1. התקנת הכלים

### Windows 10/11
1. הפעילי WSL2 (פעם אחת): פתחי PowerShell **כמנהל** והריצי:
   ```powershell
   wsl --install
   ```
   הפעילי מחדש את המחשב.
2. הורידי והתקיני **Docker Desktop** מ-https://www.docker.com/products/docker-desktop/ (בהתקנה השאירי מסומן "Use WSL 2").
3. ב-Docker Desktop → Settings → Resources: תני לפחות **6GB זיכרון**.
4. התקיני Git: https://git-scm.com/download/win
5. מומלץ לעבוד בתוך טרמינל של **WSL (Ubuntu)** – הכול רץ שם מהר יותר.

### macOS
1. התקיני Docker Desktop (בחרי Apple Silicon או Intel לפי המחשב).
2. Settings → Resources → Memory: לפחות 6GB.
3. Git כבר מותקן ברוב המקים (`git --version`). אם לא: `xcode-select --install`.

### Linux (Ubuntu)
<div dir="ltr">

```bash
sudo apt-get update && sudo apt-get install -y git ca-certificates curl
curl -fsSL https://get.docker.com | sudo sh      # Docker Engine + Compose plugin (חינמי)
sudo usermod -aG docker $USER                    # כדי להריץ docker בלי sudo; התנתקי והתחברי שוב
```

</div>

### בדיקה שהכול מותקן
<div dir="ltr">

```bash
docker --version
docker compose version
git --version
```

</div>
פלט צפוי (בערך):
<div dir="ltr">

```
Docker version 29.x.x
Docker Compose version v2.x / v5.x
git version 2.x
```

</div>

---

## 2. הפעלת Docker

- **Windows/Mac:** פתחי את Docker Desktop וחכי שהאייקון יגיד "Engine running".
- **Linux:** `sudo systemctl start docker`

בדיקה:
<div dir="ltr">

```bash
docker run --rm hello-world
```

</div>
צפוי: `Hello from Docker!` ✅

---

## 3. הורדת הפרויקט והגדרת סודות

<div dir="ltr">

```bash
git clone https://github.com/katya-alferov/AdPulse.git
cd AdPulse
cp .env.example .env
```

</div>

פתחי את `.env` בעורך ושני את שתי הסיסמאות:
<div dir="ltr">

```
POSTGRES_PASSWORD=<סיסמה משלך>
S3_SECRET_KEY=<סיסמה משלך>
```

</div>
> 🔒 הקובץ `.env` **לא** עולה ל-Git (הוא ב-`.gitignore`). רק `.env.example`, שיש בו ערכים לדוגמה, נשמר במאגר.

---

## 4. הפעלת כל השירותים

<div dir="ltr">

```bash
docker compose up -d --build
```

</div>
- `up` = הפעילי את השירותים, `-d` = ברקע, `--build` = בני את האימג'ים שלנו.
- בפעם הראשונה זה לוקח 5–10 דקות (הורדת Kafka, Spark, PostgreSQL וכו').

בדיקת מצב:
<div dir="ltr">

```bash
docker compose ps -a
```

</div>
פלט צפוי (אחרי כדקה):
<div dir="ltr">

```
adpulse-api              Up (healthy)
adpulse-auditor          Up
adpulse-dashboard        Up
adpulse-generator        Up
adpulse-kafka            Up (healthy)
adpulse-kafka-init       Exited (0)      ← תקין! הוא יוצר topics ומסיים
adpulse-postgres         Up (healthy)
adpulse-seaweedfs        Up (healthy)
adpulse-spark-streaming  Up
adpulse-storage-init     Exited (0)      ← תקין! הוא יוצר bucket ומסיים
```

</div>

---

## 5. יצירת אירועים (Generator)

ה-generator מתחיל לבד ושולח כ-40 חשיפות בשנייה (ועוד קליקים, המרות ואירועים "מלוכלכים").
לראות אותו עובד:
<div dir="ltr">

```bash
docker compose logs -f generator
```

</div>
פלט צפוי כל 10 שניות:
<div dir="ltr">

```
adpulse.generator | sent=2624 delivered=2619 failed=0 | imp=1976 clk=48 conv=3 dup=42 invalid=105 late=69 bot_clicks=450
```

</div>
(Ctrl+C יוצא מהלוגים – השירות ממשיך לרוץ.)

לשנות את הקצב: ערכי `EVENTS_PER_SECOND` ב-`.env` ואז `docker compose up -d generator`.

---

## 6. בדיקה שהאירועים נכנסים ל-Kafka

**כמה הודעות יש בכל partition:**
<div dir="ltr">

```bash
docker exec adpulse-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic ad-events
```

</div>
צפוי (המספרים גדלים כל הזמן):
<div dir="ltr">

```
ad-events:0:1015
ad-events:1:1098
ad-events:2:977
```

</div>

**לראות 3 הודעות אמיתיות:**
<div dir="ltr">

```bash
docker exec adpulse-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic ad-events --from-beginning --max-messages 3 --property print.key=true --property print.partition=true
```

</div>

**מצב ה-consumer group של ה-auditor (כולל lag):**
<div dir="ltr">

```bash
docker exec adpulse-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group adpulse-auditor
```

</div>
`LAG` קטן = הצרכן עומד בקצב. ✅

**אירועים פסולים שהגיעו ל-Dead Letter Queue:**
<div dir="ltr">

```bash
docker exec adpulse-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic ad-events-dlq --from-beginning --max-messages 2
```

</div>
צפוי:
<div dir="ltr">

```
{"error": "invalid_event_type", "source_partition": 2, "source_offset": 3, "raw_value": "{\"event_id\":...\"event_type\":\"hover\"..."}
```

</div>

---

## 7. בדיקה ש-Spark מעבד

<div dir="ltr">

```bash
docker compose logs -f spark-streaming | grep adpulse
```

</div>
צפוי כל ~20 שניות (כל micro-batch):
<div dir="ltr">

```
[metrics] batch 4: upserted 65 rows
[duplicates] batch 4: upserted 15 rows
[invalid] batch 4: 7 invalid events
[fraud] batch 5: upserted 3 rows
```

</div>
**Spark UI** (גרפים של כל query): http://localhost:4040 → לשונית **Structured Streaming**.

---

## 8. בדיקת קבצי Parquet באחסון S3 המקומי

<div dir="ltr">

```bash
docker compose run --rm --no-deps storage-init python -m adpulse.storage.object_store --list --prefix events/ | tail -5
```

</div>
צפוי:
<div dir="ltr">

```
  14523  events/event_date=2026-11-10/event_type=click/part-00000-....snappy.parquet
  ...
{'parquet_files': 81, 'bytes': 1360896}
```

</div>
שימי לב למבנה `event_date=.../event_type=...` – זה **partitioning**: Spark יכול לקרוא רק את התיקייה של יום מסוים או סוג מסוים.

לקרוא Parquet עם Python (מהמחשב, אחרי `pip install -r requirements/dev.txt`):
<div dir="ltr">

```python
import pyarrow.dataset as ds, pyarrow.fs as fs
s3 = fs.S3FileSystem(endpoint_override="http://localhost:8333", access_key="adpulse_local_key",
                     secret_key="<S3_SECRET_KEY מה-.env>", scheme="http")
table = ds.dataset("adpulse-lake/events", filesystem=s3, format="parquet", partitioning="hive").to_table()
print(table.num_rows); print(table.slice(0, 5).to_pandas())
```

</div>

---

## 9. שאילתות SQL ב-PostgreSQL

<div dir="ltr">

```bash
docker exec -it adpulse-postgres psql -U adpulse -d adpulse
```

</div>
ואז, למשל:
<div dir="ltr">

```sql
-- ביצועי קמפיינים
SELECT campaign_id, impressions, clicks, ctr_pct, cvr_pct FROM v_campaign_performance ORDER BY ctr_pct DESC;

-- CTR לפי דקה (ה-window של Spark)
SELECT window_start, SUM(impressions) imp, SUM(clicks) clk,
       ROUND(100.0*SUM(clicks)/NULLIF(SUM(impressions),0),2) ctr
FROM metrics_minute GROUP BY window_start ORDER BY window_start DESC LIMIT 10;

-- חשודים בהונאת קליקים
SELECT window_start, user_id, campaign_id, click_count FROM suspicious_activity ORDER BY click_count DESC LIMIT 5;

-- למה אירועים נפסלו
SELECT validation_error, SUM(event_count) FROM invalid_events_minute GROUP BY 1 ORDER BY 2 DESC;
```

</div>
פלט אמיתי לדוגמה:
<div dir="ltr">

```
 campaign_id | impressions | clicks | ctr_pct | cvr_pct
 cmp_003     |        1442 |    160 |   11.10 |    3.75
```

</div>
יציאה: `\q`

---

## 10. הדשבורד

פתחי בדפדפן: **http://localhost:8501**
- **Overview** – KPI-ים, חשיפות וקליקים לפי דקה, CTR לפי קמפיין
- **Campaigns** – טבלת ביצועים + drill-down לקמפיין
- **Publishers** – ביצועי אתרים + דירוג (מהעבודה ה-batch)
- **Fraud & Quality** – חשודים, כפילויות, אירועים פסולים

הדשבורד מתרענן לבד כל 10 שניות.

---

## 11. בדיקת ה-REST API

תיעוד אינטראקטיבי: **http://localhost:8000/docs** (אפשר ללחוץ "Try it out").

מהטרמינל:
<div dir="ltr">

```bash
curl -s localhost:8000/health
curl -s localhost:8000/api/v1/summary
curl -s localhost:8000/api/v1/campaigns/cmp_003
curl -s "localhost:8000/api/v1/timeseries?minutes=15"
curl -s "localhost:8000/api/v1/suspicious?limit=5"
curl -s -o /dev/null -w "%{http_code}\n" localhost:8000/api/v1/campaigns/cmp_999   # 404
```

</div>
פלט אמיתי:
<div dir="ltr">

```json
{"status":"ok","database":true}
{"impressions":16735,"clicks":1102,"conversions":39,"spend_usd":619.78,"revenue_usd":3391.49,"ctr_pct":6.59,"conversion_rate_pct":3.54,...}
```

</div>

---

## 12. עבודת ה-Batch (דירוג publishers)

<div dir="ltr">

```bash
docker compose run --rm spark-batch
```

</div>
צפוי: טבלה עם `rank_in_campaign` ובסוף `Wrote 48 publisher ranking rows to PostgreSQL`. התוצאה מופיעה בדשבורד בעמוד Publishers.

---

## 13. הרצת הבדיקות האוטומטיות

<div dir="ltr">

```bash
python -m venv .venv && source .venv/bin/activate      # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements/dev.txt
pytest tests/unit -q                                   # צפוי: 59 passed
docker compose run --rm --no-deps spark-streaming python3 -m pytest tests/spark -q -p no:cacheprovider   # 8 passed
pytest -m integration tests/integration -v             # 7 passed (המערכת צריכה לרוץ)
```

</div>

---

## 14. עצירה, הפעלה מחדש וניקוי

| פקודה | מה קורה |
|---|---|
| `docker compose stop` | עוצר, הכול נשמר |
| `docker compose start` | ממשיך מאיפה שעצר (Spark ממשיך מה-checkpoint!) |
| `docker compose down` | מוחק קונטיינרים, **הנתונים נשמרים** ב-volumes |
| `docker compose down -v` | מוחק גם את כל הנתונים (Kafka, Postgres, S3, checkpoints) – התחלה נקייה |
| `docker compose restart spark-streaming` | מפעיל מחדש שירות אחד |

---

## 15. פתרון תקלות

| תסמין | סיבה אפשרית | פתרון |
|---|---|---|
| `port is already allocated` | פורט 5432/8000/8501 תפוס (למשל Postgres מקומי) | עצרי את התוכנה האחרת, או שני את הפורט השמאלי ב-`docker-compose.yml` (למשל `"5433:5432"`) |
| `POSTGRES_PASSWORD ... required` | אין `.env` | `cp .env.example .env` |
| spark-streaming מתאתחל שוב ושוב | חסר זיכרון | Docker Desktop → Resources → 6GB+; `docker compose logs spark-streaming` |
| הדשבורד: "Cannot reach the API" | ה-API עוד עולה | חכי 30 שניות; `docker compose ps api` |
| הדשבורד ריק | Spark עוד לא סיים batch ראשון | חכי 1–2 דקות |
| `exec format error` | ארכיטקטורת מעבד | כל האימג'ים תומכים ב-amd64 ו-arm64; הריצי `docker compose build --no-cache` |
| `no space left on device` | הדיסק של Docker מלא | `docker system prune` (מוחק אימג'ים לא בשימוש) |
| שיניתי סיסמה ב-`.env` ו-Postgres לא מתחבר | הסיסמה נשמרת רק ביצירה הראשונה של ה-volume | `docker compose down -v` ואז `up` מחדש |
| אחרי שינוי קוד לא רואים שינוי | האימג' ישן | `docker compose up -d --build <service>` |

פקודות דיבאג שימושיות:
<div dir="ltr">

```bash
docker compose logs --tail=50 <service>     # לוגים של שירות
docker exec -it adpulse-postgres sh         # להיכנס לקונטיינר
docker stats                                # כמה CPU/זיכרון כל קונטיינר צורך
```

</div>

---

## 16. מצב דמו (בלי Docker)

<div dir="ltr">

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

</div>
הדשבורד ייפתח עם snapshot סינתטי שיוצא מהרצה אמיתית של המערכת – זה מה שמשמש לדמו ציבורי (ראו `PORTFOLIO_DEPLOYMENT_HE.md`).

</div>
