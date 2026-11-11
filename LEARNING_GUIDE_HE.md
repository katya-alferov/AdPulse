<div dir="rtl">

# 📚 מדריך הלמידה של AdPulse

> המדריך הזה כתוב כאילו אני מסבירה לילדה בת 10, חכמה מאוד, שמעולם לא שמעה על Kafka או Spark.
> בכל מושג: קודם **הסבר פשוט**, אחר כך **ההסבר הטכני**, ואז **איפה זה ב-AdPulse**.
> בחלק השני יש הסבר על **כל קובץ חשוב** בפרויקט: מה הוא עושה, כל import, כל פונקציה, שורות חשובות, שגיאות נפוצות.

## תוכן עניינים

**חלק א' – הסיפור**
1. [מה זה AdPulse ואיך עובד פרסום באינטרנט](#1)

**חלק ב' – מושגים**
2. [Big Data ו-Distributed Computing](#2)
3. [Apache Kafka: Topic, Producer, Consumer, Partition, Consumer Group, Offset](#3)
4. [Apache Spark: PySpark, SparkSession, DataFrame, Lazy Evaluation, Transformations/Actions](#4)
5. [Batch מול Real-time ו-Structured Streaming](#5)
6. [Event Time, Watermark, אירועים מאחרים וכפילויות](#6)
7. [Docker, Containers, Docker Compose](#7)
8. [Cloud Storage, AWS S3, MinIO/SeaweedFS, IAM ו-Parquet](#8)
9. [PostgreSQL ו-SQL](#9)
10. [REST API ו-FastAPI](#10)
11. [Git ו-GitHub](#11)

**חלק ג' – הקוד, קובץ אחרי קובץ**
12. [מבנה הפרויקט](#12)
13. [config.py](#13) · [models.py](#14) · [metrics.py](#15) · [validation.py](#16)
14. [catalog.py + event_factory.py](#17) · [generator/__main__.py](#18)
15. [kafka_io/admin.py](#19) · [producer.py](#20) · [consumer.py](#21)
16. [Spark: schemas, transformations, sinks, session, streaming_job, batch_report_job](#22)
17. [SQL: 01_schema.sql](#23) · [storage/db.py + object_store.py](#24)
18. [API: main.py](#25) · [Dashboard](#26)
19. [Docker: Dockerfiles + docker-compose.yml](#27)
20. [הבדיקות](#28)
21. [מסע של אירוע אחד דרך כל המערכת](#29)
22. [שגיאות נפוצות ואיך מדבגים](#30)

---

# חלק א' – הסיפור

<a id="1"></a>
## 1. מה זה AdPulse ואיך עובד פרסום באינטרנט

### הסבר פשוט
דמייני שאת קוראת כתבה באתר חדשות. בתחתית הכתבה יש קופסה: "אולי יעניין אותך: נעלי ספורט במבצע!". הקופסה הזאת היא **פרסומת**.
יש שלושה "שחקנים":
- **המפרסם (Advertiser)** – החברה שמוכרת נעליים. היא משלמת כסף כדי שיראו את המודעה.
- **המפרסם-האתר (Publisher)** – אתר החדשות. הוא מקבל כסף על כך שהוא מציג מודעות.
- **פלטפורמת הפרסום** – כמו Taboola. היא מחליטה איזו מודעה להראות למי, וסופרת הכול.

שלושה רגעים חשובים:
| אירוע | מה קרה | דוגמה |
|---|---|---|
| **Impression** (חשיפה) | המודעה הוצגה על המסך | 1,000 אנשים ראו את מודעת הנעליים |
| **Click** (קליק) | מישהו לחץ על המודעה | 30 מהם לחצו |
| **Conversion** (המרה) | מי שלחץ גם קנה / נרשם | 3 קנו נעליים |

### שני המדדים הכי חשובים
- **CTR** (Click-Through Rate) = קליקים ÷ חשיפות × 100 → `30 / 1000 × 100 = 3%`. "כמה המודעה מושכת".
- **Conversion Rate** = המרות ÷ קליקים × 100 → `3 / 30 × 100 = 10%`. "כמה הקליקים שווים משהו".

### למה צריך מערכת כמו AdPulse?
בחברה כמו Taboola יש **מיליארדי** חשיפות ביום. צריך:
1. לקלוט את כל האירועים בלי לאבד אף אחד (כסף!).
2. לנקות אירועים שבורים ו**כפולים** (אם קליק נספר פעמיים – המפרסם משלם פעמיים, וזה לא הוגן).
3. לזהות **הונאה** – רובוט שלוחץ 20 פעם בדקה כדי "לגנוב" כסף מהמפרסם.
4. לחשב מדדים **בזמן אמת** – כדי שמפרסם יראה עכשיו שהקמפיין שלו לא עובד ויעצור אותו.
5. לשמור הכול לטווח ארוך לניתוחים.

זה בדיוק מה ש-AdPulse עושה – עם נתונים **מומצאים** (סינתטיים).

### התמונה הגדולה
<div dir="ltr">

```
מחולל אירועים → Kafka → Spark → (S3 Parquet + PostgreSQL) → FastAPI → Streamlit
   "המפעל"     "הדואר"  "המוח"       "המחסן"   "הספרייה"     "החלון"   "המסך"
```

</div>

---

# חלק ב' – מושגים

<a id="2"></a>
## 2. Big Data ו-Distributed Computing

**פשוט:** תחשבי שצריך לספור את כל גרגרי האורז בשק ענק. לבד – זה ייקח שבוע. אם תחלקי את השק ל-100 חברים, כל אחד סופר קערה קטנה, ובסוף מחברים את המספרים – זה ייקח שעה. זה **חישוב מבוזר**.

**טכני:**
- **Big Data** = נתונים שגדולים מדי, מהירים מדי או מגוונים מדי למחשב אחד (ה-3V: Volume, Velocity, Variety).
- **Distributed Computing** = חלוקת העבודה בין הרבה מחשבים (nodes) שעובדים במקביל. צריך לטפל ב: חלוקת נתונים, תיאום, ומה קורה כשמחשב אחד נופל.
- Kafka מבזר **אחסון וקליטה** של הודעות; Spark מבזר **חישוב**.

**ב-AdPulse:** ה-topic `ad-events` מחולק ל-3 partitions (3 "קערות"), ו-Spark מעבד אותם במקביל עם כמה threads (`local[*]` = כל הליבות). בענן אמיתי אותו קוד היה רץ על עשרות מחשבים.

---

<a id="3"></a>
## 3. Apache Kafka

### מה זה Kafka? 📬
**פשוט:** Kafka הוא כמו **סניף דואר ענק וחכם**. מי ששולח מכתבים (Producer) שם אותם בתיבה. מי שקורא (Consumer) לוקח אותם מתי שנוח לו. המכתבים **לא נזרקים** אחרי שקראו אותם – הם נשמרים לזמן מוגדר, כך שאפשר לקרוא שוב.

**טכני:** Kafka היא פלטפורמת **event streaming** מבוזרת: יומן (log) של הודעות שנכתבות לסוף בלבד (append-only), שמור על דיסק, משוכפל בין שרתים (brokers). היא מפרידה בין מי שמייצר נתונים למי שצורך אותם (decoupling).

### Topic 📂
**פשוט:** "מגירה" עם שם. כל המכתבים מאותו סוג הולכים לאותה מגירה.
**ב-AdPulse:** `ad-events` (כל האירועים) ו-`ad-events-dlq` (אירועים פגומים – "מגירת תלונות").

### Producer ✉️
**פשוט:** מי ששולח מכתבים.
**טכני:** לקוח שכותב הודעות ל-topic. כל הודעה = **key** (אופציונלי) + **value** (התוכן, אצלנו JSON).
**ב-AdPulse:** `AdEventProducer` ב-`kafka_io/producer.py`. הגדרות חשובות:
- `acks=all` – ה-broker מאשר רק אחרי שההודעה נשמרה בבטחה.
- `enable.idempotence=true` – אם יש שליחה חוזרת בגלל תקלת רשת, Kafka לא ישמור את אותה הודעה פעמיים.
- `linger.ms=20` – מחכים עד 20ms כדי לשלוח הודעות בקבוצה (batch) – הרבה יותר יעיל.

### Partition 🗂️
**פשוט:** המגירה מחולקת ל-3 תאים. כך 3 עובדים יכולים לקרוא במקביל – כל אחד מתא אחר.
**טכני:** topic מחולק ל-partitions; כל partition הוא log מסודר. **הסדר נשמר רק בתוך partition**. ההודעה נכנסת ל-partition לפי `hash(key) % num_partitions`.
**ב-AdPulse:** ה-key הוא `user_id` → כל האירועים של אותו משתמש באותו partition → הם נשארים **בסדר הנכון** (החשיפה לפני הקליק).

### Offset 🔖
**פשוט:** מספר סידורי של כל מכתב בתא – כמו מספר עמוד בספר. ה-consumer זוכר "קראתי עד עמוד 1023" כדי שאם יפסיק – ימשיך משם.
**טכני:** offset הוא מיקום ההודעה ב-partition. Consumer **מבצע commit** ל-offset כדי לשמור התקדמות. מתי עושים commit קובע את ההבטחה:
- commit **אחרי** עיבוד → **at-least-once** (במקרה של קריסה – הודעה עלולה להיות מעובדת פעמיים, אבל לא תאבד). ← זה מה שעשינו.
- commit **לפני** עיבוד → at-most-once (עלולים לאבד).

### Consumer ו-Consumer Group 👥
**פשוט:** צוות של קוראים עם שם משותף. Kafka מחלק ביניהם את התאים: אם יש 3 תאים ו-3 קוראים – כל אחד מקבל תא. אם אחד הולך הביתה, האחרים לוקחים את התא שלו (**rebalance**).
**טכני:** consumers עם אותו `group.id` חולקים את ה-partitions; כל partition נקרא ע"י consumer **אחד** בקבוצה. קבוצות שונות קוראות את אותם נתונים **באופן בלתי תלוי** – לכל אחת offsets משלה.
**ב-AdPulse:**
- הקבוצה `adpulse-auditor` (ה-consumer שלנו ב-Python) בודקת תקינות ושולחת פגומים ל-DLQ.
- Spark קורא את **אותו topic** באופן עצמאי, ושומר offsets ב-checkpoint שלו.
- אפשר להריץ 3 auditors: `docker compose up -d --scale auditor=3` (צריך להסיר את `container_name` כדי לעשות scale) – ו-Kafka יחלק להם את 3 ה-partitions.

### Dead Letter Queue (DLQ) ☠️
**פשוט:** מכתב שאי אפשר לקרוא (כתובת חסרה, דף קרוע) לא זורקים – שמים במגירה מיוחדת כדי שמישהו יבדוק.
**ב-AdPulse:** `ad-events-dlq`. כל רשומה כוללת את הסיבה, ה-partition וה-offset המקוריים, והטקסט המקורי.

### KRaft
גרסאות ישנות של Kafka היו צריכות שירות נוסף בשם ZooKeeper. מ-Kafka 3.3 ואילך יש **KRaft** – Kafka מנהל את עצמו. אנחנו מריצים broker אחד שהוא גם controller (`KAFKA_PROCESS_ROLES: broker,controller`).

---

<a id="4"></a>
## 4. Apache Spark

### מה זה Spark? 🧠
**פשוט:** Spark הוא "מנהל עבודה" שיודע לקחת משימה ענקית (לחשב CTR על מיליארד אירועים), לחלק אותה להרבה חתיכות קטנות, לתת לכל "פועל" חתיכה, ולחבר את התוצאות.
**טכני:** מנוע חישוב מבוזר בזיכרון. יש **Driver** (המנהל – מתכנן את העבודה) ו-**Executors** (הפועלים – מבצעים tasks על partitions של הנתונים). אצלנו `local[*]` = Driver ו-Executors באותו תהליך, thread לכל ליבה.

### PySpark
ה-API של Spark בשפת Python. Spark עצמו כתוב ב-Scala ורץ ב-JVM (Java); PySpark מתקשר איתו דרך ספרייה בשם **Py4J**. לכן צריך Java בקונטיינר של Spark.

### SparkSession 🚪
**פשוט:** הדלת הראשית לכל מה ש-Spark יודע לעשות.
**טכני:** האובייקט שמחזיק את ההגדרות ונותן `spark.read`, `spark.readStream`, `spark.sql`.
**ב-AdPulse:** `adpulse_spark/session.py` – בונה session עם הגדרות UTC, מעט shuffle partitions, וחיבור ל-S3.

### DataFrame 📊
**פשוט:** טבלה (כמו באקסל או ב-SQL) – אבל שיכולה להיות מפוזרת על 100 מחשבים.
**טכני:** אוסף מבוזר של שורות עם **schema** (שמות וסוגי עמודות). פעולות כמו `select`, `filter`, `groupBy`, `join` – כמעט אחד-לאחד כמו SQL. Spark מתרגם אותן לתוכנית ביצוע אופטימלית (Catalyst optimizer).

### Lazy Evaluation 😴
**פשוט:** את אומרת לאמא: "תכיני רשימת קניות: חלב, לחם, ביצים". היא כותבת – אבל לא הולכת לסופר עד שאת אומרת "יאללה, לך!". ובזכות זה שהיא רואה את כל הרשימה מראש, היא מתכננת מסלול חכם בסופר.
**טכני:** Spark לא מבצע כלום כשאת כותבת transformations – רק בונה **תוכנית** (DAG). הביצוע קורה רק ב-**action**. היתרון: אופטימיזציה של כל התוכנית יחד (למשל דחיפת filter מוקדם).

### Transformations מול Actions
| Transformations (מתכננות, עצלניות) | Actions (מפעילות!) |
|---|---|
| `select`, `filter`, `withColumn`, `groupBy().agg()`, `join`, `dropDuplicates`, `withWatermark` | `count()`, `collect()`, `show()`, `write...`, `writeStream...start()` |
| מחזירות DataFrame חדש | מחזירות תוצאה / כותבות |

**ב-AdPulse:** כל הפונקציות ב-`transformations.py` הן transformations. ב-`streaming_job.py`, רק `.start()` מתחיל את העבודה בפועל.

### Narrow מול Wide (Shuffle)
- **Narrow**: כל שורה מטופלת לבד (`filter`, `withColumn`) – אין צורך להעביר נתונים בין מחשבים.
- **Wide / Shuffle**: צריך לאסוף שורות עם אותו key למקום אחד (`groupBy`, `join` גדול) – יקר, כי עובר ברשת. לכן הגדרנו `spark.sql.shuffle.partitions=4` (ברירת המחדל 200 מתאימה לקלאסטר, לא ללפטופ).

### Broadcast Join
טבלת הקמפיינים קטנה (8 שורות). במקום לעשות shuffle, Spark שולח **עותק שלם** שלה לכל executor (`F.broadcast(...)`), וכל אחד מצרף לבד. מהיר מאוד.

---

<a id="5"></a>
## 5. Batch מול Real-time ו-Structured Streaming

**פשוט:**
- **Batch** = כמו לכבס את כל הכביסה פעם בשבוע. מחכים שהערימה תתמלא ואז מעבדים הכול.
- **Streaming** = כמו מדיח כלים שמנקה כל צלחת ברגע שהיא נכנסת.

**טכני:**
| | Batch | Streaming |
|---|---|---|
| נתונים | סופיים (קובץ, טבלה) | אינסופיים (זרם מ-Kafka) |
| זמן תגובה | דקות-שעות | שניות |
| דוגמה ב-AdPulse | `batch_report_job.py` – דירוג publishers מכל ה-Parquet | `streaming_job.py` – מדדים כל 20 שניות |

### Structured Streaming
הרעיון הגאוני של Spark: **זרם = טבלה שכל הזמן מתווספות לה שורות**. כותבים את אותו קוד DataFrame כמו ב-batch, ו-Spark מריץ אותו שוב ושוב על הנתונים החדשים (**micro-batches**).
מושגים:
- **Trigger** – כל כמה זמן מריצים micro-batch (אצלנו 20 שניות).
- **Checkpoint** – תיקייה שבה Spark שומר איזה offsets כבר עיבד, ואת ה-**state** (חלונות פתוחים, event_ids שכבר נראו). אם הקונטיינר נופל – ממשיכים בדיוק מאותה נקודה.
- **Output modes**:
  - `append` – כותבים רק שורות חדשות שלא ישתנו (אצלנו: קבצי Parquet).
  - `update` – כותבים רק שורות שהשתנו ב-batch הזה (אצלנו: מדדי חלונות → upsert ל-Postgres).
  - `complete` – כותבים את כל הטבלה כל פעם (לא בשימוש – יקר).
- **foreachBatch** – "תני לי כל micro-batch כ-DataFrame רגיל ואני אעשה איתו מה שאני רוצה" (אצלנו: UPSERT ל-PostgreSQL).

---

<a id="6"></a>
## 6. Event Time, Watermark, אירועים מאחרים וכפילויות

### Event Time מול Processing Time ⏰
**פשוט:** את שולחת גלויה מחו"ל ב-1 ביולי (**זמן האירוע**), והיא מגיעה הביתה ב-10 ביולי (**זמן העיבוד**). אם רוצים לדעת "מתי היית בחופשה" – מסתכלים על התאריך על הגלויה, לא על יום ההגעה.
**טכני:** `event_time` = מתי זה קרה במכשיר של המשתמש. processing time = מתי Spark ראה את זה. טלפון שהיה בלי קליטה ב-10 דקות ישלח קליק "ישן". אם נספור לפי processing time – הקליק ייספר בדקה הלא נכונה.
**ב-AdPulse:** החלונות מחושבים לפי `event_ts` (event time). ה-generator שולח בכוונה ~3% אירועים מאחרים ב-1–14 דקות.

### Window (חלון זמן) 🪟
`F.window("event_ts", "1 minute")` – כל אירוע נכנס ל"דלי" של הדקה שבה קרה: 10:00–10:01, 10:01–10:02... זה נקרא **tumbling window** (חלונות צמודים שלא חופפים).

### Watermark 🌊
**פשוט:** המורה אוספת מבחנים. היא אומרת: "אחכה לאיחורים עד 10 דקות. מי שמגיע אחרי זה – לא נבדק". כך היא יכולה בסוף לסגור את הערימה ולתת ציונים.
**טכני:** `withWatermark("event_ts", "10 minutes")` אומר ל-Spark: watermark = (זמן האירוע המקסימלי שראיתי) − 10 דקות. 
- אירוע עם זמן **אחרי** ה-watermark → נכנס לחלון שלו ומעדכן אותו (late but OK).
- אירוע **לפני** ה-watermark → נזרק (too late).
- חלונות ו-state ישנים מה-watermark **נמחקים מהזיכרון** – אחרת הזיכרון היה גדל לנצח.

**הוכחה שזה עובד:** הבדיקה `test_streaming_watermark_drops_too_late_events` ב-`tests/spark/test_transformations.py` מריצה stream אמיתי: אחרי שראינו אירוע ב-10:20 (watermark=10:10), אירוע מ-10:00 **לא** נספר, ואירוע מ-10:15 **כן** נספר.

### Deduplication (הסרת כפילויות) 🔁
**למה יש כפילויות?** רשת לא יציבה: הטלפון שלח קליק, לא קיבל אישור, ושלח שוב. מבחינת השרת – שני קליקים.
**הפתרון:** לכל אירוע יש `event_id` ייחודי. `dropDuplicatesWithinWatermark(["event_id"])` שומר רק את הראשון, וזוכר event_ids רק בטווח ה-watermark (כדי לא למלא את הזיכרון).
**בנוסף** אנחנו **מתעדים** כפילויות בטבלה `duplicate_events` – כדי שצוות הנתונים יראה כמה כפילויות מגיעות ומאיפה.

### זיהוי הונאת קליקים 🤖
כלל: אותו `user_id` + `ip_address` לחץ על אותו קמפיין **8 פעמים או יותר בדקה** → חשוד. אדם אמיתי לא לוחץ 8 פעמים בדקה על אותה מודעה. ה-generator מייצר "בוטים" (`bot_001`...) שלוחצים 10–25 פעמים בכמה שניות.
**למה זה חשוב לחברת פרסום?** מפרסם משלם לפי קליק. קליקים מזויפים = גניבה. פלטפורמה שלא מסננת אותם מאבדת אמון ולקוחות.

---

<a id="7"></a>
## 7. Docker, Containers, Docker Compose

### Container 📦
**פשוט:** דמייני קופסת אוכל שבתוכה יש את המנה **וגם** את הצלחת, הסכו"ם והמלח. בכל מקום שתפתחי אותה – תוכלי לאכול בדיוק אותו דבר. Container הוא "קופסה" שבתוכה התוכנה + כל מה שהיא צריכה (ספריות, גרסת Python, Java...).
**טכני:** תהליך מבודד שחולק את ה-kernel של מערכת ההפעלה, עם מערכת קבצים, רשת ומשאבים משלו. נבנה מ-**Image** (תבנית לקריאה בלבד, שכבות-שכבות) לפי **Dockerfile** (מתכון).
- **Image** = המתכון האפוי (קפוא). **Container** = מנה שהוצאת מהמקפיא ומחממת עכשיו.

### Docker מול Virtual Machine
| | Container | VM |
|---|---|---|
| מה מבודד | תהליך | מחשב שלם כולל מערכת הפעלה |
| גודל | עשרות-מאות MB | GBs |
| עלייה | שניות | דקות |
| בידוד | טוב | חזק יותר |

### Docker Compose 🎼
**פשוט:** מנצח על תזמורת. במקום להגיד לכל נגן מתי להתחיל – יש תווים (`docker-compose.yml`) וכולם מתחילים בסדר הנכון.
**טכני:** קובץ YAML שמגדיר כמה services, הרשת שלהם, volumes, משתני סביבה ותלויות. פקודה אחת: `docker compose up`.

מושגים שאנחנו משתמשים בהם:
- **Network** (`adpulse-net`) – כל הקונטיינרים ברשת פרטית אחת ומוצאים זה את זה **לפי שם** (`kafka:9092`, `postgres:5432`). זה DNS פנימי של Docker.
- **Ports** (`"8000:8000"`) – "חור בקיר" מהמחשב שלך לקונטיינר. בלי זה – אי אפשר לגשת מהדפדפן.
- **Volumes** (`postgres-data`) – תיקיות שנשמרות גם אם הקונטיינר נמחק. בלי זה – כל הנתונים היו נעלמים ב-`down`.
- **Environment variables / env_file** – הגדרות וסודות מבחוץ (`.env`), לא בקוד.
- **Healthcheck** – פקודה ש-Docker מריץ כדי לדעת אם השירות באמת **מוכן** (לא רק "רץ"). למשל `pg_isready` ל-Postgres.
- **depends_on + condition** – "אל תתחיל את Spark עד ש-Kafka healthy ו-kafka-init סיים בהצלחה".
- **restart: unless-stopped** – אם השירות קורס, Docker מפעיל אותו מחדש לבד.
- **profiles** – שירות שלא עולה כברירת מחדל (`spark-batch` רץ רק כשמבקשים).

---

<a id="8"></a>
## 8. Cloud Storage, AWS S3, MinIO/SeaweedFS, IAM ו-Parquet

### Object Storage ☁️
**פשוט:** מחסן ענק עם מיליוני ארגזים. כל ארגז (**object**) יש לו מדבקה עם שם (**key**), והוא נמצא באולם מסוים (**bucket**). אין "מדפים בתוך מדפים" באמת – השם `events/event_date=2026-11-10/part-0.parquet` הוא פשוט מדבקה ארוכה שנראית כמו תיקיות.
**טכני:** אחסון של קבצים (objects) דרך HTTP API. זול מאוד, כמעט אינסופי, עמיד (S3 מבטיח 99.999999999% עמידות). לא מתאים לשאילתות מהירות על שורה בודדת – מתאים לכמויות ענק של קבצים.

### AWS S3
השירות של אמזון (Simple Storage Service). ה-API שלו הפך ל"שפה" סטנדרטית – הרבה מוצרים מדברים "S3".
- **Bucket** – מיכל עם שם ייחודי עולמית (אצלנו `adpulse-lake`).
- **Object** – קובץ + metadata.
- **Key** – השם המלא של הקובץ.
- **Region** – באיזה אזור גיאוגרפי הנתונים נשמרים.

### IAM ו-Credentials 🔑
**פשוט:** תעודת עובד שקובעת לאיזה חדרים מותר לך להיכנס.
**טכני:** IAM (Identity and Access Management) ב-AWS מגדיר **מי** (user/role) רשאי לעשות **מה** (s3:GetObject, s3:PutObject) על **איזה משאב** (bucket). 
- מקומית אנחנו משתמשים ב-**access key + secret key** (כמו שם משתמש וסיסמה), שנמצאים ב-`.env`.
- ב-AWS אמיתי, הנוהג הנכון: **IAM Role** שמוצמד לשרת – בלי מפתחות קבועים בכלל, והרשאות מינימליות (least privilege).

### MinIO → SeaweedFS (החלטה הנדסית חשובה!)
ביקשת MinIO, ובדקתי את המצב העדכני:
- ב-**אוקטובר 2025** MinIO הפסיקו לפרסם אימג'ים ובינאריים חינמיים לגרסת הקהילה.
- ב-**2026** המאגר שלהם עבר לארכיון (maintenance/archived) – אין עדכוני אבטחה.
- להשתמש בשרת אחסון שלא מתוחזק – לא החלטה הנדסית טובה.

לכן בחרתי ב-**SeaweedFS**: רישיון Apache-2.0 (חינמי לגמרי), מתוחזק פעיל, ותומך ב-S3 API. **הקוד שלנו לא יודע ולא אכפת לו** – הוא מדבר S3 (`boto3`, `s3a://`). להחליף ל-MinIO או ל-AWS S3 = לשנות `S3_ENDPOINT_URL` ומפתחות. זו בדיוק **הסיבה** שמשתמשים ב-API סטנדרטי. (זה גם משפט מצוין לראיון!)

### Parquet 🧱
**פשוט:** דמייני טבלה של תלמידים. CSV שומר שורה-שורה: "דנה, 10, ירושלים". Parquet שומר **עמודה-עמודה**: כל השמות יחד, כל הגילאים יחד. אם רוצים רק ממוצע גילאים – קוראים רק את עמודת הגילאים!
**טכני:** פורמט **עמודתי** (columnar), דחוס (snappy), עם schema מובנה וסטטיסטיקות (min/max) לכל בלוק.
- קורא רק עמודות נדרשות (**column pruning**).
- מדלג על בלוקים לא רלוונטיים (**predicate pushdown**).
- דחיסה מעולה כי ערכים דומים יושבים יחד.
- **Partitioning** בתיקיות: `event_date=2026-11-10/event_type=click/` → שאילתה על יום אחד קוראת רק תיקייה אחת.

### איך אותה ארכיטקטורה הייתה נראית ב-AWS (תיאוריה בלבד – לא פרסנו כלום!)
| מקומי | AWS |
|---|---|
| SeaweedFS | Amazon S3 |
| Kafka בקונטיינר | Amazon MSK |
| Spark בקונטיינר | EMR / EMR Serverless / Glue |
| PostgreSQL בקונטיינר | Amazon RDS |
| FastAPI/Streamlit | ECS Fargate / App Runner |
| `.env` עם מפתחות | IAM Roles + Secrets Manager |
> ⚠️ AWS Free Tier **לא** אומר "חינם בלי הגבלה". MSK ו-EMR בכלל לא בחינם. לכן לא פרסנו שום דבר לענן.

---

<a id="9"></a>
## 9. PostgreSQL ו-SQL

**פשוט:** ספרייה מסודרת. כל ספר במקום שלו, יש קטלוג (אינדקס), ואפשר למצוא תשובה מהר.
**טכני:** מסד נתונים רלציוני, ACID (טרנזקציות אמינות), SQL מלא. מתאים ל**שאילתות מהירות על נתונים מסוכמים** – בדיוק מה שה-API צריך.

אצלנו (את מכירה את זה מ-DWH! זה **star schema** קטן):
- **Dimensions**: `campaigns`, `publishers` – תיאור.
- **Facts**: `metrics_minute` – מספרים לפי (דקה, קמפיין, publisher). מפתח ראשי מורכב.
- **Quality**: `suspicious_activity`, `duplicate_events`, `invalid_events_minute`.
- **Batch output**: `publisher_rankings`.
- **Views**: `v_campaign_performance`, `v_publisher_performance` – SQL מוכן שה-API קורא.
- **Foreign keys** – `metrics_minute.campaign_id REFERENCES campaigns` → אי אפשר להכניס קמפיין שלא קיים.

**UPSERT** (`INSERT ... ON CONFLICT DO UPDATE`) – "אם השורה קיימת – עדכני, אם לא – הוסיפי". כמו `MERGE` באורקל/MSSQL שאת מכירה.

### S3 מול PostgreSQL – למה צריך את שניהם?
| | S3 + Parquet | PostgreSQL |
|---|---|---|
| מה שומרים | **כל** האירועים הגולמיים | רק סיכומים |
| נפח | עצום וזול | קטן יחסית |
| שאילתה טיפוסית | "תחשב מחדש הכול מ-2025" (Spark) | "מה ה-CTR של קמפיין 3 עכשיו?" (מילישניות) |
| מקבילה בעולם ה-DWH | Data Lake / staging | Data Mart |

---

<a id="10"></a>
## 10. REST API ו-FastAPI

**פשוט:** מלצר במסעדה. את (הדשבורד) לא נכנסת למטבח (מסד הנתונים). את אומרת למלצר "אני רוצה את סיכום הקמפיינים", והוא מביא לך צלחת מסודרת (JSON).
**טכני:** REST = סגנון של API מעל HTTP:
- **Resource** עם כתובת (URL): `/api/v1/campaigns/cmp_003`
- **Method**: `GET` (לקרוא), `POST` (ליצור), `PUT/PATCH` (לעדכן), `DELETE`
- **Status codes**: `200` הצלחה, `404` לא נמצא, `422` קלט לא תקין, `503` השירות לא זמין
- **Stateless** – כל בקשה עומדת בפני עצמה.

**FastAPI:** ספריית Python מהירה שבונה API מ-type hints. היא נותנת **בחינם**: בדיקת קלט (`minutes` חייב להיות 1–1440), המרת פלט לפי מודל (Pydantic), ותיעוד אינטראקטיבי ב-`/docs`.

### Kafka מול REST API
| | REST | Kafka |
|---|---|---|
| תקשורת | בקשה-תשובה, סינכרונית | שלח ושכח, אסינכרונית |
| מי מחכה | הלקוח מחכה לתשובה | אף אחד לא מחכה |
| אם הצד השני למטה | הבקשה נכשלת | ההודעה מחכה ב-Kafka |
| שימוש ב-AdPulse | דשבורד ← API | generator → Spark |

---

<a id="11"></a>
## 11. Git ו-GitHub
הסבר מלא ב-`GIT_GUIDE_HE.md`. בקצרה: **Git** = מכונת זמן לקוד (כל commit = תמונת מצב). **GitHub** = כספת בענן לגיבוי ושיתוף.

---

# חלק ג' – הקוד, קובץ אחרי קובץ

<a id="12"></a>
## 12. מבנה הפרויקט – ולמה ככה

<div dir="ltr">

```
src/adpulse/      ← קוד Python של השירותים (generator, auditor, API, dashboard)
spark_jobs/       ← קוד PySpark (רץ בקונטיינר אחר, עם Java)
sql/init/         ← יצירת טבלאות
tests/            ← unit / spark / integration
docker/           ← Dockerfiles
```

</div>
**למה לא קובץ אחד?** כל מודול עם אחריות אחת (Single Responsibility). אפשר לבדוק כל חלק לבד, להחליף חלק בלי לשבור אחרים, וכמה אנשים יכולים לעבוד במקביל.

**למה `src/` layout?** מונע מצב שבו בדיקות "עובדות" רק כי הן רצות מהתיקייה הנכונה. הקוד נטען כ-package (`adpulse.*`).

---

<a id="13"></a>
## 13. `src/adpulse/config.py` – הגדרות ממשתני סביבה

**מה ולמה:** כל השירותים צריכים לדעת "איפה Kafka? מה הסיסמה ל-Postgres?". במקום לכתוב את זה בקוד (מסוכן ולא גמיש) – קוראים ממשתני סביבה.

**Imports:**
| import | למה |
|---|---|
| `from __future__ import annotations` | מאפשר לכתוב type hints כמו `"Settings"` לפני שהמחלקה הוגדרה |
| `os` | `os.getenv("NAME")` קורא משתנה סביבה |
| `dataclass, field` | יוצר מחלקות נתונים בלי לכתוב `__init__` ידנית |
| `Optional` | type hint: "ערך או None" |

**מחלקות ופונקציות:**
- `ConfigError` – שגיאה משלנו, כדי שיהיה ברור שהבעיה בהגדרות.
- `_get_str / _get_int / _get_float` – קוראות משתנה, ממירות לסוג, ובודקות טווח. `_get_float("DUPLICATE_RATE", 0.02, 0.0, 1.0)` → אם מישהו כתב `1.5` – שגיאה ברורה מיד בהפעלה ("fail fast"), ולא באג מוזר אחרי שעה.
- `PostgresSettings`, `KafkaSettings`, `S3Settings`, `GeneratorSettings` – קבוצות הגדרות.
- `password: str = field(default="", repr=False)` ← **שורה חשובה**: `repr=False` אומר שכשמדפיסים את האובייקט ללוג – הסיסמה **לא** תופיע. יש על זה בדיקה (`test_password_is_not_printed`).
- `@dataclass(frozen=True)` – אי אפשר לשנות הגדרות אחרי יצירה (מונע באגים).
- `Settings.from_env()` – `@classmethod` שבונה את כל ההגדרות. דוגמה:
  ```python
  settings = Settings.from_env()
  print(settings.kafka.bootstrap_servers)   # "kafka:9092" בתוך Docker
  ```
**מאחורי הקלעים:** Docker Compose קורא את `.env` (`env_file: .env`) ומכניס כל שורה כמשתנה סביבה לקונטיינר.
**שגיאה נפוצה:** `ConfigError: EVENTS_PER_SECOND must be a number` → יש טעות הקלדה ב-`.env`.

---

<a id="14"></a>
## 14. `src/adpulse/models.py` – מבנה האירוע

**מה ולמה:** מגדיר "איך נראה אירוע". כל המערכת מדברת באותם שמות שדות.
- `EventType(str, Enum)` – רק 3 ערכים חוקיים: impression/click/conversion. ירושה מ-`str` מאפשרת להשוות ישירות למחרוזת.
- `REQUIRED_FIELDS` – tuple של שדות חובה (משמש ב-validation).
- `to_iso(ts)` – ממיר זמן ל-`2026-11-10T21:00:00.123Z` (ISO-8601, UTC). **למה UTC?** משתמשים בכל העולם; אם כל אחד ישמור לפי השעון שלו – בלגן.
- `AdEvent` (dataclass) – השדות: `event_id` (UUID ייחודי), `event_time` (זמן האירוע), `campaign_id`, `publisher_id`, `user_id` (סינתטי!), `ip_address` (טווח פרטי 10.x – לא IP אמיתי של אף אחד), `cost_usd`, `revenue_usd`, `parent_event_id` (קליק → החשיפה שלו), `sent_at`.
- `to_json()` – `separators=(",", ":")` = JSON בלי רווחים מיותרים (חוסך בייטים × מיליארדים).
- `kafka_key` (property) → `user_id`. ראו הסבר partition למעלה.

---

<a id="15"></a>
## 15. `src/adpulse/metrics.py` – נוסחאות עסקיות

<div dir="ltr">

```python
def safe_ratio_percent(numerator, denominator, digits=2):
    if denominator is None or denominator <= 0 or numerator is None:
        return 0.0
    return round(numerator / denominator * 100.0, digits)
```

</div>
**שורה חשובה:** הבדיקה `denominator <= 0` – קמפיין חדש עם 0 חשיפות היה גורם ל-`ZeroDivisionError` ומפיל את ה-API. במקום זה: 0%.
- `ctr`, `conversion_rate`, `cpc` (עלות לקליק = הוצאה ÷ קליקים), `roas` (Return On Ad Spend = הכנסה ÷ הוצאה; 3.0 = כל דולר החזיר 3).
- ב-SQL עשינו אותו דבר עם `NULLIF(SUM(impressions), 0)` – חלוקה ב-NULL מחזירה NULL ולא שגיאה.

---

<a id="16"></a>
## 16. `src/adpulse/validation.py` – בדיקת תקינות

**מה ולמה:** "שומר בכניסה". אירוע זבל שנכנס למדדים = מדדים שגויים = החלטות עסקיות שגויות = כסף.
- `@dataclass(frozen=True) class ValidationResult` – תוצאה: תקין? סיבה? האירוע המפוענח.
- `parse_event_time(value)` – `datetime.fromisoformat(value.replace("Z", "+00:00"))`. ה-`replace` כי גרסאות Python ישנות לא מבינות `Z`.
- `validate_event(raw, now=None)`: הכללים לפי הסדר (מחזיר את **הראשון** שנכשל):
  1. JSON תקין ושהוא אובייקט → `malformed_json`
  2. שדות חובה קיימים ולא ריקים → `missing_field:<name>`
  3. event_type מוכר → `invalid_event_type`
  4. זמן ניתן לפענוח → `invalid_event_time`
  5. לא יותר מ-5 דקות בעתיד → `event_time_in_future` (שעון שגוי במכשיר / זיוף)
  6. סכומים לא שליליים → `negative_amount`
- **הפרמטר `now`** – מאפשר לבדיקות לקבוע "מה השעה" ולקבל תוצאה קבועה (testability).
- **אירוע מאחר הוא לא פסול!** הוא תקין – Spark יחליט לפי ה-watermark אם עוד אפשר לספור אותו.
- **אותם כללים בדיוק** כתובים גם ב-Spark (`validation_error_expr`). למה פעמיים? ה-auditor עובד על הודעה בודדת ב-Python; Spark עובד על מיליונים כ-DataFrame. (בפרויקט גדול היינו מגדירים את הכללים פעם אחת ב-schema registry / חוזה נתונים.)

---

<a id="17"></a>
## 17. `generator/catalog.py` + `generator/event_factory.py`

**catalog.py:** 8 קמפיינים ו-6 publishers **מומצאים** (StrideWear, SkyHop Travel...). לכל קמפיין `base_ctr` ו-`base_cvr` – ההסתברות שחשיפה תהפוך לקליק, וקליק להמרה. אותם נתונים נכנסים ל-Postgres ב-`02_seed_dimensions.sql`, ויש בדיקה (`test_seed_matches_catalog`) שמוודאת ששני המקומות זהים.

**event_factory.py – המפעל:**
| import | למה |
|---|---|
| `random` | הגרלות. `random.Random(seed)` – מחולל עם seed משלו |
| `uuid` | מזהים ייחודיים |
| `deque` | רשימה עם גודל מקסימלי – "זוכרת" רק את 2000 החשיפות האחרונות |
| `timedelta` | חישובי זמן (לאחר 5 דקות אחורה) |

- `self.rng = random.Random(self.settings.seed)` ← **seed** = אותה התחלה → אותם מספרים "אקראיים". בבדיקות זה מאפשר תוצאות שחוזרות על עצמן.
- `_uuid()` – יוצר UUID מה-rng (ולא מ-`uuid4()`) כדי שגם המזהים יהיו דטרמיניסטיים עם seed.
- `_event_time(now)` – ב-`late_event_rate` מהמקרים מחזיר זמן של 1–14 דקות אחורה. **בכוונה** חלק בתוך ה-watermark (10 דק') וחלק מחוצה לו.
- `make_impression / make_click / make_conversion` – הקליק מקבל `parent_event_id` של החשיפה, אותו משתמש ו-IP; ההמרה – של הקליק. עלות הקליק = `cpc_usd` של הקמפיין.
- `make_invalid` – מייצר אחד מ-5 סוגי זבל (שדה חסר, סוג שגוי, JSON חתוך, זמן עתידי, סכום שלילי).
- `make_bot_burst` – משתמש `bot_XXX` שלוחץ 10–25 פעם תוך 3 שניות.
- `next_batch(now)` – "טיק" אחד: חשיפה → אולי קליק (לפי CTR) → אולי המרה (לפי CVR) → אולי בוט → אולי זבל → אולי כפילות של אירוע אחרון (**אותו event_id בדיוק**).

---

<a id="18"></a>
## 18. `generator/__main__.py` – הרצת המחולל

- קובץ בשם `__main__.py` מאפשר להריץ package: `python -m adpulse.generator`.
- `GeneratorRunner.run()`: `interval = 1 / events_per_second`. בכל איטרציה: מייצר batch, שולח, ישן את הזמן שנשאר. כל 10 שניות – שורת סטטיסטיקה ללוג.
- `signal.signal(signal.SIGTERM, runner.stop)` – כש-Docker עוצר קונטיינר הוא שולח SIGTERM. אנחנו תופסים אותו, יוצאים מהלולאה ועושים `flush()` – כדי **לא לאבד** הודעות שעוד בתור.
- `ensure_topics(...)` בהתחלה – גם אם מריצים את ה-generator לבד, ה-topics קיימים.

---

<a id="19"></a>
## 19. `kafka_io/admin.py` – יצירת Topics

- `AdminClient` – לקוח ניהול של Kafka. `NewTopic(name, num_partitions, replication_factor, config)`.
- `replication_factor=1` – יש broker אחד. **בפרודקשן: 3**, כדי ש-broker שנופל לא יאבד נתונים.
- `retention.ms` – כמה זמן לשמור: יום ל-events, שבוע ל-DLQ (צריך זמן לחקור).
- לולאת retry – בהפעלה Kafka עוד לא מוכן; מנסים 30 פעם עם הפסקה של 2 שניות.
- `TOPIC_ALREADY_EXISTS` – לא שגיאה. הפונקציה **idempotent**: אפשר להריץ 100 פעם, התוצאה זהה.
- למה לא auto-create? (`KAFKA_AUTO_CREATE_TOPICS_ENABLE: false`) – טעות הקלדה בשם topic הייתה יוצרת topic חדש בשקט. עדיף שגיאה.

---

<a id="20"></a>
## 20. `kafka_io/producer.py` – שליחה ל-Kafka

- `serialize(payload)` → `(key, value)` כ-bytes. Kafka מעביר **bytes** בלבד – הוא לא יודע מה זה JSON.
- `producer_factory=Producer` – **Dependency Injection**: בבדיקות מעבירים `FakeProducer` ובודקים לוגיקה בלי Kafka אמיתי.
- `produce(...)` **לא שולח מיד** – מכניס לתור מקומי. ספריית librdkafka שולחת ברקע בקבוצות.
- `on_delivery=self._on_delivery` – callback שנקרא כשה-broker אישר (או נכשל). כך סופרים delivered/failed.
- `except BufferError: self._producer.poll(0.5)` – התור המקומי מלא (שולחים מהר מדי) → מחכים קצת שיתפנה. זה **back-pressure**.
- `poll(0)` – "תריץ callbacks שמחכים" בלי לחכות.
- `flush()` – חוסם עד שכל ההודעות נשלחו. חובה לפני יציאה.

---

<a id="21"></a>
## 21. `kafka_io/consumer.py` – ה-Auditor (Consumer Group)

<div dir="ltr">

```python
Consumer({"bootstrap.servers": ..., "group.id": "adpulse-auditor",
          "auto.offset.reset": "earliest", "enable.auto.commit": False})
```

</div>
- `group.id` – שם הקבוצה.
- `auto.offset.reset: earliest` – קבוצה **חדשה** (בלי offsets שמורים) מתחילה מההתחלה.
- `enable.auto.commit: False` – אנחנו מחליטים מתי לעשות commit.

הלולאה:
<div dir="ltr">

```python
msg = self.consumer.poll(1.0)        # חכה עד שנייה להודעה
...
self.handle(msg.value(), msg.partition(), msg.offset())
self.consumer.commit(message=msg, asynchronous=True)   # רק אחרי עיבוד → at-least-once
```

</div>
- `on_assign` – callback שמדפיס אילו partitions קיבלנו (רואים rebalance בלוג).
- `handle()` – מאמת; פסול → `build_dlq_record` → שליחה ל-DLQ.
- `finally: consumer.close()` – עוזב את הקבוצה מסודר, כדי ש-Kafka יעשה rebalance מיד ולא יחכה ל-timeout.

**איך לראות consumer groups בפעולה:** הריצי `kafka-consumer-groups.sh --describe --group adpulse-auditor` (ב-RUN_PROJECT_HE.md) – תראי לכל partition: CURRENT-OFFSET, LOG-END-OFFSET, LAG.

---

<a id="22"></a>
## 22. קוד ה-Spark

### `adpulse_spark/schemas.py`
`StructType([StructField("event_id", StringType()), ...])` – ה-schema של ה-JSON. **למה להגדיר מראש?** בלי schema Spark צריך לסרוק נתונים כדי לנחש – איטי, ובזרם אי אפשר לנחש. `event_time` נקרא כמחרוזת וממיר אחר כך – כדי שזמן לא תקין יהפוך ל-NULL שנוכל לזהות, ולא יפיל את הכול.

### `adpulse_spark/settings.py`
אותו רעיון כמו `config.py` – הגדרות מסביבה. `jdbc_url` = `jdbc:postgresql://postgres:5432/adpulse` (כתובת שה-Java driver מבין).

### `adpulse_spark/session.py`
| הגדרה | למה |
|---|---|
| `spark.sql.session.timeZone=UTC` | כל הזמנים ב-UTC |
| `spark.sql.shuffle.partitions=4` | ברירת מחדל 200 = 200 tasks קטנטנים מיותרים על לפטופ |
| `fs.s3a.endpoint` | לאן לשלוח בקשות S3 (SeaweedFS במקום AWS) |
| `fs.s3a.path.style.access=true` | כתובת בסגנון `http://host/bucket/key` (שרתים מקומיים צריכים את זה) |
| `fs.s3a.access.key/secret.key` | מגיע מ-`.env` |
`setLogLevel("ERROR")` – Spark מדבר **הרבה**. משאירים רק שגיאות.

### `adpulse_spark/transformations.py` – הלב ❤️
`from pyspark.sql import functions as F` – כל הפונקציות של Spark SQL (`F.col`, `F.when`, `F.window`...). הקונבנציה `F` מקובלת בתעשייה.

1. **`parse_events(raw)`**
   - `F.col("value").cast("string")` – Kafka נותן bytes; ממירים לטקסט.
   - `F.from_json(value_str, EVENT_SCHEMA)` – מפענח JSON לפי schema. JSON שבור → כל השדות NULL (לא קריסה!).
   - שומרים `partition/offset/timestamp` של Kafka – מאוד שימושי לדיבאג ("איזו הודעה בדיוק?").
   - `F.to_timestamp("event_time")` → עמודה `event_ts` מסוג timestamp.
2. **`validation_error_expr()`** – בונה **ביטוי `CASE WHEN` אחד** בלולאה. Spark מריץ אותו על כל שורה בצורה וקטורית. שימי לב: `F.when(...).when(...).otherwise(None)` – בדיוק כמו `CASE WHEN ... WHEN ... ELSE NULL END` ב-SQL.
3. **`validate_and_enrich(events, campaigns, publishers)`** – `join(F.broadcast(camp), on="campaign_id", how="left")`. **left** join כדי לא לאבד אירועים – אם הקמפיין לא נמצא, `campaign_name` יהיה NULL → מסמנים `unknown_campaign`. (inner join היה מעלים אותם בשקט – באג קלאסי!)
4. **`split_valid_invalid`** – שני DataFrames: תקינים (בלי עמודת השגיאה) ופסולים (עם הטקסט המקורי).
5. **`with_event_time_watermark`** – `withWatermark("event_ts", "10 minutes")`.
6. **`deduplicate`** – ב-stream: `dropDuplicatesWithinWatermark(["event_id"])` (Spark 3.5+). ב-batch (בדיקות): `dropDuplicates`. בודקים עם `df.isStreaming`.
7. **`aggregate_metrics`**
   ```python
   events.groupBy(F.window("event_ts", "1 minute"), "campaign_id", "publisher_id").agg(
       F.sum(F.when(F.col("event_type") == "impression", 1).otherwise(0)).alias("impressions"), ...)
   ```
   הטריק `SUM(CASE WHEN type='click' THEN 1 ELSE 0 END)` – סופר כמה סוגים במעבר אחד (pivot ידני). את מכירה את זה מ-SQL!
8. **`detect_suspicious_clicks`** – רק קליקים → groupBy(דקה, user, ip, campaign) → `count >= threshold`.
9. **`detect_duplicates`** – groupBy(חלון 10 דק', event_id) → `count > 1`. רץ על הנתונים **לפני** ה-dedup (אחרת לא היו כפילויות לראות).
10. **`lake_columns`** – בוחר עמודות ל-Parquet ומוסיף `event_date` ל-partitioning.

### `adpulse_spark/sinks.py` – כתיבה ל-PostgreSQL
- `psycopg2` + `execute_values` – הכנסה של אלפי שורות בפקודה אחת (מהיר פי 50 משורה-שורה).
- `METRICS_UPSERT` – `ON CONFLICT (window_start, campaign_id, publisher_id) DO UPDATE SET impressions = EXCLUDED.impressions`. `EXCLUDED` = השורה שניסינו להכניס.
- **למה מחליפים ולא מוסיפים?** ב-output mode `update`, Spark שולח את **הערך המלא העדכני** של החלון. החלפה = **idempotent**: אם Spark מריץ batch שוב אחרי קריסה – אותה תוצאה. (חוץ מ-invalid counts שהם חיבוריים – מתועד כ-at-least-once.)
- `with conn, conn.cursor() as cur:` – `with conn` = טרנזקציה: הכול נכנס או כלום.
- `batch_writer(...)` מחזיר **פונקציה** (closure) שמתאימה ל-`foreachBatch(func(df, batch_id))`.
- `.collect()` – מביא את השורות ל-Driver. מותר כי אלה **סיכומים** קטנים (עשרות שורות לבאצ'). על נתונים גולמיים – אסור!

### `spark_jobs/streaming_job.py`
1. `build_spark` → `load_dimensions` (קריאת `campaigns`/`publishers` ב-JDBC פעם אחת, `.cache()` בזיכרון).
2. `read_kafka`: `startingOffsets=earliest` (רק בהרצה ראשונה; אחר כך ה-checkpoint קובע), `maxOffsetsPerTrigger=20000` (לא לבלוע מיליון הודעות בבת אחת אחרי השבתה), `failOnDataLoss=false`.
3. בונים גרף אחד משותף (lazy!) ו**6 queries**:
   | query | מקור | mode | יעד |
   |---|---|---|---|
   | events_to_lake | clean | append | Parquet `events/` (partitionBy date, type) |
   | metrics_to_postgres | clean → window | update | `metrics_minute` |
   | fraud_to_postgres | clean → clicks | update | `suspicious_activity` |
   | duplicates_to_postgres | valid (לפני dedup) | update | `duplicate_events` |
   | invalid_to_lake | invalid | append | Parquet `invalid_events/` (הסגר) |
   | invalid_counts_to_postgres | invalid | foreachBatch | `invalid_events_minute` |
4. לכל query `checkpointLocation` נפרד – חובה! כל query זוכר offsets ו-state משלו.
5. `spark.streams.awaitAnyTermination()` – רץ לנצח; אם query אחד נכשל – התוכנית יוצאת, ו-Docker (`restart: unless-stopped`) מפעיל מחדש מה-checkpoint.

**באג אמיתי שתיקנו (סיפור טוב לראיון!):** בהתחלה כתבנו אירועים פסולים ל-S3 בתוך `foreachBatch` עם `df.write.parquet`. זה נכשל שוב ושוב עם `FileNotFoundException ... _temporary`. הסיבה: ה-committer הרגיל של Hadoop כותב לתיקייה זמנית ואז עושה **rename** – ובאחסון אובייקטים אין באמת rename (זה copy+delete), ותיקיות הן אשליה. הפתרון: להשתמש ב-**file sink של Structured Streaming** (שכותב קבצים ישירות ומנהל manifest ב-`_spark_metadata`) – כמו שכבר עשינו ל-events.

### `spark_jobs/batch_report_job.py` + `batch_logic.py`
- `spark.read.parquet("s3a://adpulse-lake/events")` – קורא את **כל** ה-data lake.
- `compute_rankings`: groupBy(campaign, publisher) → `Window.partitionBy("campaign_id").orderBy(F.desc("clicks"))` → `F.rank().over(...)`. זה בדיוק `RANK() OVER (PARTITION BY campaign_id ORDER BY clicks DESC)` מ-SQL.
- כתיבה: `TRUNCATE` + `INSERT` **באותה טרנזקציה** – הדשבורד אף פעם לא רואה טבלה ריקה.

---

<a id="23"></a>
## 23. `sql/init/01_schema.sql`
- רץ אוטומטית כש-Postgres עולה **בפעם הראשונה** (כל קובץ ב-`/docker-entrypoint-initdb.d` לפי סדר א"ב → `01_` לפני `02_`).
- `CHECK (daily_budget_usd >= 0)` – כלל עסקי ברמת המסד.
- `PRIMARY KEY (window_start, campaign_id, publisher_id)` – מפתח מורכב = הבסיס ל-UPSERT.
- `CREATE INDEX idx_metrics_campaign ON metrics_minute (campaign_id, window_start)` – מאיץ "תני לי את קמפיין X לאורך זמן".
- Views עם `LEFT JOIN` מהדימנסיה – קמפיין בלי נתונים עדיין מופיע (עם 0), ו-`NULLIF` נגד חלוקה באפס.

---

<a id="24"></a>
## 24. `storage/db.py` + `storage/object_store.py`

**db.py – Repository Pattern:** כל ה-SQL במקום אחד. ה-API לא יודע SQL – הוא קורא `repo.campaigns()`.
- `class AnalyticsRepository(Protocol)` – "חוזה": כל מחלקה עם המתודות האלה מתאימה. בבדיקות – `FakeRepo`.
- `%(minutes)s` – **פרמטרים**, לא f-string! מונע **SQL Injection** (מישהו שולח `campaign_id = "x'; DROP TABLE campaigns;--"`).
- `row_factory=dict_row` – כל שורה חוזרת כ-dict (`{"clicks": 30}`) – נוח ל-JSON.
- `_jsonable` – Postgres מחזיר `Decimal`; JSON לא מכיר אותו → ממירים ל-int/float.
- `(%(cid)s::text IS NULL OR campaign_id = %(cid)s)` – פילטר אופציונלי בשאילתה אחת.

**object_store.py:**
- `boto3.client("s3", endpoint_url=...)` – **אותו קוד בדיוק** ל-AWS (פשוט `endpoint_url=None`).
- `addressing_style: path` – כמו `path.style.access` ב-Spark.
- `ensure_bucket` – `head_bucket` (קיים?) → אם 404 → `create_bucket`. idempotent + retries.
- `get_paginator("list_objects_v2")` – S3 מחזיר עד 1000 objects בכל קריאה; paginator עובר על כל הדפים.

---

<a id="25"></a>
## 25. `api/main.py` – FastAPI

- `app = FastAPI(title=..., version=...)` – האפליקציה.
- `@lru_cache(maxsize=1) def get_repository()` – יוצר repository **פעם אחת** (singleton).
- `repo: AnalyticsRepository = Depends(get_repository)` – **Dependency Injection** של FastAPI. בבדיקות: `app.dependency_overrides[get_repository] = lambda: FakeRepo()`.
- `@app.get("/api/v1/campaigns/{campaign_id}", response_model=CampaignPerformance)` – `{campaign_id}` = path parameter; `response_model` = FastAPI מוודא שהתשובה במבנה הנכון ומתעד אותה.
- `minutes: int = Query(60, ge=1, le=24*60)` – ולידציה: `?minutes=0` → **422** אוטומטית.
- `raise HTTPException(status_code=404, ...)` – קמפיין לא קיים.
- `@app.exception_handler(Exception)` – כל שגיאה לא צפויה (DB נפל) → 503 עם הודעה כללית, ובלוג – הפרטים. **לא** חושפים stack trace למשתמש (אבטחה).
- `/api/v1/` – **versioning**: אם נשנה את ה-API בעתיד, נוסיף `/v2` בלי לשבור לקוחות קיימים.

`api/schemas.py` – מודלי Pydantic (`class Summary(BaseModel)`) – כל שדה עם סוג.

---

<a id="26"></a>
## 26. הדשבורד – `dashboard/app.py`, `api_client.py`, `demo_source.py`

- **הדשבורד לא מדבר עם ה-DB** – רק עם ה-API (כמו front-end אמיתי). כך אפשר להחליף את ה-DB בלי לגעת בדשבורד.
- `AdPulseClient._get` – `requests.get(..., timeout=5)` (**תמיד timeout!** בלי – הדשבורד יכול להיתקע לנצח), `raise_for_status()` הופך 4xx/5xx לשגיאה, ו-`ApiError` אחיד.
- `@st.cache_resource` – יוצר את הלקוח פעם אחת ולא בכל רענון.
- `@st.fragment(run_every=10)` – רק החלק הזה מתרענן כל 10 שניות.
- `px.area / px.line / px.bar` – גרפים של Plotly (עם tooltip בריחוף).
- צבעים: פלטה כהה שנבדקה בכלי בדיקה לעיוורון צבעים על רקע `#1b1c30`. **אין גרף עם שני צירי Y** (חשיפות וקליקים בשני גרפים נפרדים – כי הסקאלות שונות מאוד).
- **מצב דמו** (`ADPULSE_MODE=demo`): `DemoDataSource` עם **אותן מתודות בדיוק** כמו `AdPulseClient` (duck typing / strategy pattern), אבל קורא JSON מ-`demo_data/`. כך אותו דשבורד רץ גם בלי Kafka/Spark – בשביל דמו ציבורי חינמי.

---

<a id="27"></a>
## 27. Docker – הקבצים

### `docker/app.Dockerfile`
<div dir="ltr">

```dockerfile
FROM python:3.11-slim                 # בסיס: לינוקס קטן + Python 3.11
ENV PYTHONUNBUFFERED=1                # לוגים מיד ל-stdout (אחרת Docker לא רואה אותם בזמן אמת)
COPY requirements/app.txt ...         # קודם רק requirements…
RUN pip install -r requirements/app.txt
COPY src/ src/                        # …ואז הקוד. למה בסדר הזה? cache של שכבות:
                                      # שינוי בקוד לא מחייב התקנה מחדש של הספריות
RUN useradd ... && USER adpulse       # לא רצים כ-root (אבטחה)
```

</div>
שלב ה-`docker/certs` – רלוונטי רק בסביבה עם proxy שבודק TLS (כמו ה-sandbox שבו בניתי). אצלך התיקייה ריקה וזה לא עושה כלום.

### `docker/spark.Dockerfile`
- `FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu` – אימג' רשמי של Apache.
- מוריד 7 JARs מ-Maven Central – **הגרסאות חייבות להתאים**: `spark-sql-kafka-0-10_2.12:3.5.7` (= גרסת Spark, = Scala 2.12), `hadoop-aws:3.3.4` (= ה-Hadoop של Spark 3.5), `aws-java-sdk-bundle:1.12.262` (מה ש-hadoop-aws 3.3.4 נבנה איתו). אי-התאמה = `ClassNotFoundException`/`NoSuchMethodError`.
- `ENV PYTHONPATH=...:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip` – **באג שתיקנו**: כשהגדרנו PYTHONPATH משלנו, דרסנו את זה של Spark ו-`import pyspark` נכשל.

### `docker-compose.yml`
- `x-app-common: &app-common` + `<<: *app-common` – **YAML anchors**: הגדרה משותפת אחת ל-6 שירותי Python (לא לחזור על עצמך).
- `KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://localhost:9094` – **מושג חשוב**: Kafka אומר ללקוחות "לאן להתחבר". קונטיינרים צריכים `kafka:9092`, תוכנה במחשב שלך צריכה `localhost:9094`. לכן שני listeners.
- `POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?Set POSTGRES_PASSWORD in .env}` – `:?` = אם המשתנה חסר, compose נכשל עם הודעה ברורה.
- `condition: service_completed_successfully` – Spark מחכה ש-`kafka-init` ו-`storage-init` **יסיימו בהצלחה**.
- SeaweedFS: `entrypoint` מריץ סקריפט שלנו שכותב קובץ הרשאות S3 **מתוך משתני סביבה** בזמן ריצה – כך הסוד לא נשמר ב-Git.

---

<a id="28"></a>
## 28. הבדיקות

| סוג | איפה | מה בודקים | צריך Docker? |
|---|---|---|---|
| Unit (59) | `tests/unit` | ולידציה, מדדים, config, מחולל, producer/consumer עם fakes, API עם FakeRepo, client, demo | לא |
| Spark (8) | `tests/spark` | parse, כללי ולידציה, join, dedup, חלונות, הונאה, דירוג, **stream אמיתי עם watermark** | כן (קונטיינר Spark) |
| Integration (7) | `tests/integration` | topics קיימים, נתונים מגיעים ל-Postgres ול-API, בוט מזוהה, כפילות מזוהה, JSON שבור מגיע ל-DLQ, Parquet ב-S3 | המערכת המלאה רצה |

מושגים:
- **Fake / Mock** – אובייקט מזויף שמתנהג כמו האמיתי (`FakeProducer`, `FakeRepo`) – בדיקה מהירה בלי תשתית.
- **`@pytest.mark.parametrize`** – אותה בדיקה עם הרבה קלטים (כל שדה חובה).
- **fixture** – הכנה משותפת (SparkSession אחד לכל הבדיקות).
- **בדיקת אינטגרציה עם marker ייחודי** – שולחים בוט בשם `it_bot_<uuid>` ומחכים שיופיע ב-`suspicious_activity`. ה-uuid מבטיח שאנחנו מוצאים **את** האירוע שלנו.

---

<a id="29"></a>
## 29. מסע של קליק אחד דרך כל המערכת 🎯

1. **10:00:05.120** – ה-generator יוצר חשיפה של `cmp_003` ("Learn Python Online") ל-`u_000042` באתר `pub_02`. הגרלה (CTR 4.2%) → גם קליק, עם `parent_event_id` של החשיפה.
2. `serialize` → key=`u_000042`, value=JSON. `produce` → תור מקומי → librdkafka שולח batch.
3. Kafka: `hash("u_000042") % 3 = 1` → **partition 1**, offset 1,024. נשמר לדיסק, `acks=all` → callback "delivered".
4. **שני קוראים בלתי תלויים:**
   - ה-auditor (group `adpulse-auditor`) מקבל את offset 1,024, מאמת → תקין → commit.
   - Spark (micro-batch הבא, עד 20 שניות) קורא offsets 1,000–1,500 מ-partition 1.
5. Spark: `from_json` → `event_ts = 10:00:05.120` → ולידציה: NULL (תקין) → broadcast join: `campaign_name = "Learn Python Online"`.
6. Watermark: ה-max event time ב-stream 10:00:20 → watermark 09:50:20. הקליק אחרי → נשאר.
7. Dedup: `event_id` לא נראה → עובר (אם הטלפון היה שולח שוב – העותק השני נעצר כאן, ונרשם ב-`duplicate_events`).
8. **4 מקומות במקביל:**
   - Parquet: `s3a://adpulse-lake/events/event_date=2026-11-10/event_type=click/part-....parquet`
   - חלון `[10:00, 10:01)` של `cmp_003 / pub_02`: clicks 6→7 → UPSERT ל-`metrics_minute`.
   - בדיקת הונאה: ל-`u_000042` רק קליק אחד בדקה → לא חשוד.
9. דפדפן: הדשבורד (כל 10 שניות) → `GET /api/v1/summary` → FastAPI → `SELECT ... FROM metrics_minute` → JSON → ה-KPI "Clicks" עולה ב-1. **כל המסע: פחות מ-30 שניות.**
10. בלילה: `spark-batch` קורא את ה-Parquet ומעדכן את הדירוג של `pub_02` בתוך `cmp_003`.

---

<a id="30"></a>
## 30. שגיאות נפוצות ואיך מדבגים 🔧

| שגיאה | מה זה אומר | איך מתקנים |
|---|---|---|
| `NoBrokersAvailable` / `Failed to resolve 'kafka:9092'` | מריצים מהמחשב עם כתובת פנימית | מהמחשב: `localhost:9094`; מתוך Docker: `kafka:9092` |
| `Coordinator load in progress` | Kafka עוד עולה | נורמלי בשניות הראשונות – יש retry |
| `ModuleNotFoundError: pyspark` | PYTHONPATH לא כולל את Spark | ראו Dockerfile של Spark |
| `AnalysisException: ... not supported with batch DataFrames` | פונקציית streaming על DataFrame רגיל | `if df.isStreaming` (כמו ב-`deduplicate`) |
| `ClassNotFoundException: ...S3AFileSystem` / `kafka` | JAR חסר / גרסה לא תואמת | ודאי שגרסאות ה-JARs תואמות ל-Spark/Hadoop |
| `FileNotFoundException ... _temporary` | rename על object storage | השתמשי ב-streaming file sink |
| הדשבורד מראה 0 | Spark לא כתב עדיין / נפל | `docker compose logs spark-streaming`; Spark UI ב-4040 |
| `psycopg.OperationalError: connection refused` | Postgres לא עלה / סיסמה שגויה | `docker compose ps postgres`; אם שינית סיסמה – `down -v` |
| `port is already allocated` | פורט תפוס | שני את הפורט השמאלי ב-compose |

**שיטת דיבאג בצעדים (לזכור לראיון!):** עקבי אחרי הנתונים לאורך הצינור – איפה הם נעלמים?
1. ה-generator שולח? (`logs generator` → delivered עולה)
2. הם ב-Kafka? (`kafka-get-offsets`)
3. Spark קורא? (Spark UI → Input Rate)
4. ה-Parquet נכתב? (`object_store --list`)
5. Postgres מתעדכן? (`SELECT max(updated_at) FROM metrics_minute`)
6. ה-API מחזיר? (`curl /api/v1/summary`)
7. הדשבורד מציג?

השלב הראשון שבו התשובה "לא" – שם הבאג.

</div>
