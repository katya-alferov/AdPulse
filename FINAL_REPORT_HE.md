# 📋 דוח סופי – AdPulse

תאריך: 10 בנובמבר 2026 · נבנה ונבדק בסביבת sandbox בענן (Linux, 4 ליבות, 15GB RAM, Docker 29.9) – **לא** על המחשב שלך.

## 1. מה עובד (נבדק בפועל)
| בדיקה | תוצאה |
|---|---|
| הפעלת כל המערכת מאפס (`docker compose down -v` → `up -d --build`) | ✅ 9 שירותים רצים, 2 שירותי init הסתיימו בהצלחה |
| יצירת אירועים | ✅ ~40 חשיפות/שנייה + קליקים, המרות, כפילויות, פסולים, מאחרים, בוטים |
| קליטה ב-Kafka | ✅ 3 partitions מתמלאים; consumer group עם lag נמוך; DLQ מקבל הודעות פגומות |
| עיבוד Spark | ✅ 6 streaming queries; micro-batch כל 20 שניות; אין קריסות |
| אחסון Parquet ב-S3 (SeaweedFS) | ✅ קבצים מחולקים לפי `event_date`/`event_type`; נקראו בהצלחה עם pyarrow |
| SQL ב-PostgreSQL | ✅ מדדים לפי דקה, views לקמפיינים/publishers, הונאה, כפילויות, פסולים |
| Spark batch (window function) | ✅ 48 שורות דירוג publishers |
| REST API | ✅ כל ה-endpoints, 404/422/503, Swagger ב-`/docs` |
| דשבורד | ✅ 4 עמודים, רענון אוטומטי; מצב דמו עובד בלי Docker |
| בדיקות יחידה | ✅ **59 passed** |
| בדיקות Spark | ✅ **8 passed** (כולל stream אמיתי שמוכיח watermark) |
| בדיקות אינטגרציה | ✅ **7 passed** (בוט מזוהה, כפילות מזוהה, JSON שבור ב-DLQ, Parquet ב-S3) |
| מצגת | ✅ 20 שקפים + הערות דובר, עברה ולידציה ורינדור |
| היסטוריית Git | ✅ commit לכל שלב; אין `.env` ואין סודות אמיתיים בהיסטוריה |

## 2. קבצים שנוצרו
**תשתית:** `docker-compose.yml`, `docker/app.Dockerfile`, `docker/spark.Dockerfile`, `docker/seaweedfs-entrypoint.sh`, `.env.example`, `.gitignore`, `Makefile`, `pyproject.toml`, `requirements.txt`, `requirements/{app,spark,dev}.txt`, `LICENSE`

**Python (`src/adpulse/`):** `config.py`, `logging_setup.py`, `models.py`, `metrics.py`, `validation.py`, `generator/{catalog,event_factory,__main__}.py`, `kafka_io/{admin,producer,consumer}.py`, `storage/{db,object_store}.py`, `api/{main,schemas}.py`, `dashboard/{app,api_client,demo_source}.py`, `streamlit_app.py`

**Spark (`spark_jobs/`):** `streaming_job.py`, `batch_report_job.py`, `adpulse_spark/{settings,session,schemas,transformations,sinks,batch_logic}.py`

**SQL:** `sql/init/01_schema.sql`, `sql/init/02_seed_dimensions.sql`

**בדיקות:** `tests/unit/` (9 קבצים), `tests/spark/` (2), `tests/integration/test_end_to_end.py`

**כלים ודמו:** `tools/export_demo_data.py`, `tools/make_sample_events.py`, `demo_data/` (snapshot + 300 אירועים לדוגמה), `docs/screenshots/*.png`, `docs/ARCHITECTURE.md`

**תיעוד בעברית:** `LEARNING_GUIDE_HE.md`, `RUN_PROJECT_HE.md`, `INTERVIEW_PREPARATION_HE.md`, `GIT_GUIDE_HE.md`, `PORTFOLIO_DEPLOYMENT_HE.md`, `FINAL_REPORT_HE.md`, `ADPULSE_PRESENTATION_HE.pptx` (+ `presentation/build_presentation.py`)

## 3. טכנולוגיות (כולן חינמיות וקוד פתוח)
Python 3.11 · Apache Kafka 3.9.1 (KRaft) · Apache Spark 3.5.7 / PySpark · SeaweedFS 4.53 (S3 API, Apache-2.0) · PostgreSQL 16 · FastAPI 0.115 · Streamlit 1.40 · Plotly · confluent-kafka 2.6.1 · boto3 · pyarrow · psycopg 3 · Docker / Docker Compose · pytest · python-pptx · Git

## 4. איך מריצים
```bash
cp .env.example .env      # ולשנות סיסמאות
docker compose up -d --build
# אחרי ~2 דקות: http://localhost:8501 (דשבורד) · http://localhost:8000/docs (API) · http://localhost:4040 (Spark UI)
```
פירוט מלא: `RUN_PROJECT_HE.md`.

## 5. מה לא הושלם / מגבלות (בכנות)
- **גיבוי GitHub:** ה-sandbox לא יכול ליצור repositories, ואין בו אימות `gh` תקף. כל עבודת ה-Git המקומית הושלמה. צריך שתיצרי repo **פרטי** ריק בשם `AdPulse` (הוראות ב-`GIT_GUIDE_HE.md`), ואז אני דוחפת אליו – או שאת דוחפת מהמחשב.
- **דמו ציבורי:** מוכן טכנית (Streamlit Community Cloud), **לא פורסם** – מחכה לאישור שלך.
- **לא נבדק על המחשב שלך:** הכול נבדק ב-sandbox לינוקס. Windows/Mac אמורים לעבוד עם Docker Desktop (כל האימג'ים זמינים ל-amd64 ול-arm64), אבל לא הרצתי שם בפועל.
- **צילום Swagger:** לא נכלל – ממשק `/docs` טוען קבצים מ-CDN שהיה חסום ב-sandbox. אצלך הוא יעבוד.
- **סקייל:** broker אחד, Spark `local[*]`, replication 1 – בכוונה, למחשב אישי.
- **Delivery semantics:** at-least-once. מדדי חלונות אידמפוטנטיים (upsert), אבל מונה האירועים הפסולים חיבורי ועלול לספור פעמיים אחרי replay.
- **כללי ולידציה כתובים פעמיים** (Python ו-Spark) – מתועד; בפרודקשן schema registry.
- **זיהוי הונאה:** tumbling window של דקה; burst שחוצה גבול דקה עלול להתפצל. sliding window – שיפור עתידי.
- **צריכת משאבים:** נמדד ~3.5GB RAM לכל המערכת (Spark לבד ~2.6GB). במחשב עם 8GB – לסגור תוכנות כבדות ולתת ל-Docker לפחות 6GB.

## 6. מושגים הכי חשובים לראיון
1. Kafka: topic, partition, key→ordering, offset, consumer group, at-least-once, DLQ
2. Spark: lazy evaluation, transformations vs actions, shuffle, broadcast join
3. Structured Streaming: micro-batch, checkpoint, output modes, foreachBatch
4. **Event time + watermark** + deduplication (הכי חשוב!)
5. Data lake (Parquet, partitioning) מול serving DB (Postgres, upsert אידמפוטנטי)
6. Docker: image/container, network, volume, healthcheck, secrets ב-env
7. הבאג של rename ב-S3 – סיפור דיבאג אמיתי
8. כנות: "בניתי את זה לבד כפרויקט לימודי"

## 7. אישור: לא הופעל שום משאב בתשלום ✅
- לא נוצר חשבון AWS ולא שום חשבון ענן אחר.
- לא נפרס שום דבר לענן; לא הופעל trial; לא נדרש כרטיס אשראי.
- לא פורסם שום דבר ציבורית (repo, דמו, אתר).
- כל הרכיבים קוד פתוח וחינמיים; Docker Desktop חינמי לשימוש אישי/לימודי.

## 8. צעדים ידניים שנשארו לך
1. ליצור repo פרטי ריק `AdPulse` ב-GitHub ולהגיד לי (או לדחוף בעצמך – `GIT_GUIDE_HE.md`).
2. להתקין Docker Desktop ולהריץ את המערכת אצלך (`RUN_PROJECT_HE.md`).
3. להחליט אם ואיך לפרסם פורטפוליו ודמו (`PORTFOLIO_DEPLOYMENT_HE.md`) – ולאשר לי במפורש.
4. לעבור על `INTERVIEW_PREPARATION_HE.md` ולתרגל את ה-pitch בקול.
