# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Văn Quốc Dũng |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602505 |
| GitHub | vvstdung89 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-00722 |

Cách tính trip: MSSV `2A202602505` có chữ `A` nên `int(MSSV)` của GUIDE báo lỗi. Tôi dùng phần số `202602505`: `202602505 % 26 = 1`, tức phần tử thứ 1 (đếm từ 0) trong danh sách 26 trip đã sắp xếp → `trip-00722`.

## Kết quả cuối

- `dataset_version` của bạn: `bc48cab9b3bd`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 686 / 143 / 190 (tổng 1019 khung; 18 / 4 / 4 trip)
- `report/verify.txt`: tạo bằng `scripts/lab.sh verify > report/verify.txt` (trên Windows, `--save-verify` không gọi được file `.sh`); kết quả `27/27 checks passed`.
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png` (kèm câu hỏi riêng), `crash_log.png` (log task đỏ thấy `trip-00722`), `crash_rerun_green.png` (DAG xanh sau khi clear), `metabase_own_question.png`.

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Khác biệt thấy ở `bronze.raw_telemetry_a` (3021 dòng) và `bronze.raw_telemetry_b` (3234 dòng):
1. Thời gian: A là `ts_utc` dạng ISO (`2025-10-07T08:00:00.014Z`); B là `timestamp_ms` epoch mili giây (`1759933801509`).
2. Tốc độ: A là `speed_mps` (m/s, ví dụ `1.213`); B là `spd_kmh` (km/h, ví dụ `55.11`).
3. Bàn đạp: A là `gas`/`brake` dạng tỉ lệ 0–1 (`0.028`); B là `throttle_pct`/`brake_pct` theo % (`2.0`).
4. Tên cột khác: `heading_deg`/`hdg`, `lat`/`latitude`, `lon`/`longitude`, `alt_m`/`altitude`, `acc_x`/`ax`, `steer`/`steering`.
5. Đồng hồ: `telemetry_clock_offset_ms` = 0 với A nhưng 1500 với B (đồng hồ thiết bị B chạy trước video 1,5 giây).

Mỗi nguồn một bảng vì hai schema khác nhau: gộp ngay buộc phải đoán cách ánh xạ cột trước khi hiểu dữ liệu, và một lỗi ánh xạ sẽ mất dấu vết. Mọi cột để dạng chữ (`VARCHAR`) để bronze ghi đúng nguyên bản, không mất gì: ví dụ có 30 ô tốc độ rỗng ở mỗi nguồn; nếu ép kiểu ngay thì chúng thành lỗi hoặc thành `NULL` không rõ nguồn gốc. Việc chuẩn hoá để silver làm, và vì bronze còn nguyên nên luôn chạy lại được. Bronze cũng nhận diện file bằng sha256: lần nạp thứ hai báo `already present`, và `history bronze.raw_telemetry_a` chỉ có 1 commit (version 0, 3021 dòng) dù tôi đã nạp hai lần.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

`stg_telemetry_b.sql` đổi tên cột về schema chung, chia `spd_kmh` cho 3,6 để ra m/s, chia `throttle_pct`/`brake_pct` cho 100, và đổi `timestamp_ms` thành timestamp UTC **sau khi trừ** `telemetry_clock_offset_ms` (1500 ms, lấy từ `sources.json` qua `bronze.video_files`). A chỉ cần ép kiểu. Hai nguồn được `union all by name`, rồi `silver_telemetry_classified.sql` gắn `reject_reason` cho từng dòng.

Bằng chứng: dòng B đầu tiên trong bronze là `1759933801509` = 14:30:01.509; sau khi trừ 1,5 giây thành **14:30:00.009**, khớp giờ bắt đầu video B (14:30:00). Check "drive B clock offset removed" PASS. Tốc độ tối đa sau chuẩn hoá là 71 km/h (A) và 100 km/h (B), hợp lý.

| drive | nhận về | duplicate | speed_missing | speed_out_of_range | sạch |
|---|---|---|---|---|---|
| drive_a | 3021 | 20 | 30 | 3 | 2968 |
| drive_b | 3234 | 20 | 30 | 3 | 3181 |

Sạch + cách ly = nhận về ở cả hai drive (2968 + 53 = 3021; 3181 + 53 = 3234). Dòng lỗi vào `silver_telemetry_rejects` (106 dòng) chứ không bị xoá để mọi dòng đều đối soát được, để biết nguồn nào đang gửi dữ liệu xấu (ví dụ cả 6 dòng `speed_out_of_range` đều là 250 m/s = 900 km/h), và để có thể xử lý lại nếu sau này đổi luật làm sạch.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` báo một khuyết tật **đã biết và chấp nhận được**: cảm biến mất tín hiệu. Lần chạy của tôi có 2 khoảng trống > 1 giây: drive_a 3073 ms (08:01:59 → 08:02:02) và drive_b 3124 ms (14:31:29 → 14:31:32). Dữ liệu còn lại vẫn đúng, chỉ thiếu một đoạn, nên test báo `WARN 2` và pipeline vẫn chạy tiếp. `in_range` thì khác: tốc độ ngoài 0–70 m/s là dữ liệu **sai**, mà đưa vào gold sẽ làm hỏng nhãn huấn luyện, nên phải `error` và dừng.

Thí nghiệm `break-gate` nới luật làm sạch `max_speed_mps` từ 70 lên 1000. Khi đó các giá trị vô lý lọt vào silver, và `dbt test` cho `PASS=5 WARN=1 ERROR=1`: test `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng**, đúng là 3 + 3 dòng 900 km/h mà bình thường bị cách ly. Khôi phục luật gốc thì về lại `PASS=6 WARN=1 ERROR=0`. Điều này cho thấy luật làm sạch và cổng chất lượng là hai lớp độc lập: khi ai đó nới hay làm hỏng lớp làm sạch, cổng vẫn chặn. Trong Airflow, `dbt_test_silver` lỗi sẽ dừng mọi task phía sau, nên gold và warehouse không bao giờ được dựng từ dữ liệu xấu.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Chạy lại partition `trip-00003` (60 khung) hai lần: trước là **1246 dòng**, dấu vân tay `6c7480dfde7e`; sau hai lần vẫn **1246 dòng**, vẫn `6c7480dfde7e` (`unchanged: true`). Mô phỏng cách sai: ghi `append` hai lần thì một trip 60 khung thành **120 dòng**, nhân đôi. `cli history silver.frames` cho thấy mỗi lần chạy lại là đúng một commit `WRITE` 60 dòng kèm predicate `trip_id = 'trip-00003'` (version 26 và 27).

Delta `overwrite` với predicate thay thế các dòng của trip đó **trong một commit nguyên tử**: commit chỉ được ghi vào `_delta_log` khi mọi file mới đã ghi xong. Nếu sập giữa chừng thì không có commit nào, người đọc vẫn thấy phiên bản cũ đầy đủ, và chạy lại sẽ cho cùng kết quả. Với "xoá rồi ghi lại", xoá và ghi là hai thao tác riêng: sập giữa hai bước thì trip đó biến mất khỏi bảng, sập lúc đang ghi thì còn lại một nửa. Ngoài ra, chỉ partition của trip đó bị đụng tới, 25 trip còn lại giữ nguyên.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung **đen** có độ sáng trung bình đúng 0,0 (6 khung ở A, 4 ở B): đó là tín hiệu chết, không có thông tin, nên bị loại (`dark_luma = 12`). Khung **tối** có độ sáng 12,8–39,6 (trung bình 30,8 ở B, so với 88–104 của khung bình thường): đó là đường hầm hay bóng râm, một điều kiện lái xe có thật, nên chỉ gắn cờ và **giữ lại** (`dark_soft = 40`; B có 61 khung như vậy). Tôi đồng ý: bỏ khung tối thì mô hình sẽ không bao giờ thấy đường hầm, đúng lúc nó cần chính xác nhất.

Tôi giữ ngưỡng **2**: ngưỡng 0 chỉ bắt khung giống hệt (A = 61), bỏ sót camera đứng hình có nhiễu nén; ngưỡng 6 coi 363/602 khung của A là trùng, tức loại cả những khung khác nhau thật. Ngưỡng phụ thuộc mạnh vào tốc độ: ở ngưỡng 2, 68,0% khung lúc 0–5 km/h bị coi là trùng (51/75), nhưng chỉ 11–14% khi trên 5 km/h. Xe càng chậm thì hai khung cách nhau 0,5 giây càng giống nhau, nên ngưỡng hợp lý hơn là thay đổi theo tốc độ (hoặc lấy mẫu theo quãng đường thay vì theo thời gian).

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Với 1019 khung đáng dùng thuộc 26 trip, chia ngẫu nhiên **theo khung hình** (70/15/15) làm **26/26 trip** nằm ở nhiều hơn một split, và **26/26 trip** có khung ở cả train lẫn test. Chia **theo trip** (gold của pipeline) thì **0 trip** bị chia cắt; test `no_straddle` của dbt bảo đảm điều này.

Các khung liền nhau trong một trip gần như giống hệt (cùng đường, cùng ánh sáng, cách nhau 0,5 giây). Nếu khung t nằm ở train và khung t + 0,5 s nằm ở test, mô hình chỉ cần "nhớ" cảnh là đoán đúng, nên điểm test cao nhưng không đo được khả năng tổng quát hoá sang cảnh mới. Khi triển khai, xe luôn gặp cảnh chưa từng thấy, nên điểm thật sẽ thấp hơn nhiều.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` là 12 ký tự đầu của sha256 trên danh sách `frame_id|split` (`publish.py`, `export_dataset`), nên nó chỉ phụ thuộc vào **nội dung** dataset: cùng khung, cùng split thì cùng version, dù chạy lúc nào, ở máy nào. Ngày giờ hay số đếm sẽ cho version mới mỗi lần chạy dù nội dung không đổi, và không chứng minh được hai dataset là một.

Kết quả của tôi: `bc48cab9b3bd` sau `step 4`; chạy lại `export-dataset` vẫn `bc48cab9b3bd`; dựng lại từ lake trống qua Airflow vẫn `bc48cab9b3bd` (1019 khung); lần chạy cuối `step all --reset` cũng vậy. Khi đổi tạm MSSV thành `2A202600000`, version thành `28d927dbdedd` với split 729 / 160 / 130 (của tôi là 686 / 143 / 190); đặt lại MSSV thì quay về `bc48cab9b3bd`. Lý do: `gold_dataset_frames.sql` xếp thứ tự trip trong mỗi drive bằng `md5(trip_id || LAB_STUDENT_ID)`, nên MSSV khác thì trip rơi vào split khác, các cặp `frame_id|split` khác, và băm khác.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Dataset `bc48cab9b3bd` (1019 khung: drive_a 436, drive_b 583) được tạo từ:
- Bảng bronze: `bronze.raw_telemetry_a` version 0, `bronze.raw_telemetry_b` version 0, `bronze.trips_raw` version 0, `bronze.video_files` version 1.
- Bảng silver: `silver.frames` version 25 (26 trip, mỗi trip một commit: version 0–25), `silver.telemetry`, `silver.frames_flagged`, `silver.frame_telemetry` version 0. Ở Bước 4, manifest ghi `silver.frames` version 27 vì thí nghiệm idempotent đã thêm 2 commit chạy lại; nhưng `dataset_version` và `manifest_sha256` vẫn giống hệt, vì nội dung không đổi.
- Video gốc: drive_a sha256 `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`, drive_b sha256 `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`.
- Luật: loại `is_black`, `is_blurry`, `is_near_dup`, `not sync_ok`; split "per trip, stratified by drive, md5 order".

3 tháng sau, để tái tạo đúng dataset này cần: hai video có đúng sha256 trên; các phiên bản bảng Delta liệt kê (đọc lại bằng time travel); luật lọc và split cùng các biến dbt (`dup_hamming`, `sync_tol_ms`, `split_train`...) và MSSV dùng để trộn split; rồi so `manifest_sha256` (`90ddfdb7…161c50`) và `dataset_version` để chứng minh kết quả trùng khớp. Lineage chỉ ghi số version chứ không ghi commit của mã nguồn, nên cần ghi thêm commit git của pipeline.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Với `LAB_FAIL_ONCE=trip-00722`, tôi trigger `day09_reset` rồi `day09_video_pipeline` (run `manual__2026-10-13T03:02:20`). Đúng **1** task chuyển đỏ: `silver_frames` map index 1 (`trip-00722`), log ghi `RuntimeError: simulated crash while extracting trip-00722; run it again` (ảnh `crash_log.png`). **7** task `upstream_failed`: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`. 29 task còn lại xanh, gồm 25 trip khác, nên không phải làm lại.

Tôi chỉ clear task con map index 1, kèm Downstream và Recursive; hộp thoại báo **Affected Tasks: 8** (1 + 7), không clear cả DAG run. `trip-00722` chạy lại thành công trong 20 giây, DAG thành `success` với **37/37** task xanh (ảnh `crash_rerun_green.png`). Bằng chứng không nhân đôi: `silver.frames` có 1246 dòng = 1246 `frame_id` khác nhau, `trip-00722` có đúng 60 khung, và lịch sử Delta chỉ có một commit cho trip này (version 25, 60 dòng, predicate `trip_id = 'trip-00722'`), vì lần sập xảy ra trước khi ghi nên không để lại gì. `verify` vẫn **27/27**, `dataset_version` vẫn **`bc48cab9b3bd`** (1019 khung).

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Hiện tại cả DAG mất khoảng 4 phút cho khoảng 10 phút video của 2 drive. 500 xe, mỗi xe vài giờ video mỗi ngày, là lượng dữ liệu lớn hơn hàng chục nghìn lần.
1. **Giải mã video (FFmpeg) chạy tuần tự trong một container**: `silver_frames` có `max_active_tis_per_dagrun=1`, Airflow `LocalExecutor` với `PARALLELISM=2`, tất cả trong một container 2,5 GB. Đây là nút cổ chai đầu tiên (slide 24). Thay bằng xử lý phân tán: Spark hoặc Ray trên một cụm worker, hoặc Airflow `KubernetesExecutor`/Celery, chạy song song theo partition (xe × ngày), dùng GPU decode.
2. **DuckDB một file, một tiến trình ghi** (`threads: 1`, `work/lakehouse.duckdb`): không cho nhiều worker ghi cùng lúc, và mọi bảng silver/gold phải đi qua một máy. Thay bằng engine phân tán đọc thẳng Delta trên object storage (Spark, Trino), dbt chạy trên engine đó (slide 22).
3. **Lưu trữ và số lượng object nhỏ**: MinIO một node, mỗi khung là một JPG riêng, và export dataset copy từng ảnh một. Ở quy mô này sẽ là hàng trăm triệu object nhỏ. Thay bằng object storage phân tán (S3/GCS hoặc MinIO nhiều node), gom khung thành shard (WebDataset/tar, Parquet chứa ảnh), và partition bảng Delta theo ngày và xe (slide 12, 23).

Ngoài ra, một DAG run cho mỗi lô sẽ có hàng nghìn task mapped; nên chạy theo ngày, theo từng xe, với backfill theo partition.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Câu hỏi: "Trip nào mất nhiều khung hình nhất khi dựng dataset, và vì sao?" (thẻ biểu đồ cột "Tỷ lệ khung bị loại theo trip (câu hỏi riêng)", ảnh `metabase_dashboard.png` và `metabase_own_question.png`).

```sql
select trip_id, drive_id, lighting, split, frames, frames_kept,
       frames - frames_kept as frames_dropped,
       round(100.0 * (frames - frames_kept) / frames, 1) as pct_dropped
from marts.mart_trip_summary
order by pct_dropped desc
```

Kết quả: năm trip bị loại nhiều nhất đều thuộc drive_a: `trip-04622` 56,7% (34/60 khung), `trip-03904` 50,0%, `trip-02086` 46,7%, `trip-08888` 40,0%, `trip-07774` 38,2%. Tính theo drive, drive_a mất 27,6% (436/602 khung giữ lại), drive_b chỉ 9,5% (583/644). Cả 26 trip đều có `lighting = Day`, nên ánh sáng không giải thích được chênh lệch. Nguyên nhân chính là khung trùng: drive_a có 149 khung trùng so với 53 của drive_b, do xe chạy chậm (68% khung lúc 0–5 km/h bị coi là trùng) và đoạn camera đứng hình 6 giây, cộng thêm các khung đen và mờ được cấy vào drive_a. Về vận hành: mỗi trip của drive_a cho ít dữ liệu huấn luyện hơn hẳn, và tỉ lệ loại cao bất thường ở một trip là dấu hiệu nên kiểm tra camera hoặc cách lấy mẫu.

## Điểm cộng: thí nghiệm riêng với `sync_tol_ms`

Câu hỏi: ngưỡng ghép frame với telemetry (`sync_tol_ms`, mặc định 150 ms trong `dbt/dbt_project.yml`) ảnh hưởng thế nào tới dataset gold? Cách làm: chạy lại `silver_frame_telemetry` và `gold_dataset_frames` với `dbt run --vars '{sync_tol_ms: N}'` cho N = 50, 150, 300, 1000. Tôi không sửa file cấu hình, nên không có gì phải khôi phục. Sau đó dựng lại với giá trị mặc định và kiểm tra: `verify` 27/27, gold 1019 khung. Bằng chứng: `report/evidence/bonus_sync_tol.json`.

| `sync_tol_ms` | `sync_lost` (A / B) | khung gold | train / val / test | độ trễ TB / max trong gold (ms) | `dataset_version` |
|---|---|---|---|---|---|
| 50 | 272 / 328 | 538 | 360 / 75 / 103 | 10 / 20 | `2da0a67fb137` |
| **150 (mặc định)** | 7 / 7 | **1019** | 686 / 143 / 190 | 53 / 120 | **`bc48cab9b3bd`** |
| 300 | 7 / 7 | 1019 | 686 / 143 / 190 | 53 / 120 | `bc48cab9b3bd` |
| 1000 | 5 / 6 | 1022 | 689 / 143 / 190 | 55 / 884 | `5c05a6a83746` |

Giải thích: độ trễ frame–telemetry không phân bố đều mà dồn thành hai cụm, 0–20 ms (646 khung) và 81–120 ms (586 khung), vì telemetry đến khoảng mỗi 77 ms còn frame lấy mẫu mỗi 500 ms. Không có khung nào trễ 121–300 ms, nên 150 và 300 cho **cùng** dataset và cùng `dataset_version`. Hạ xuống 50 ms cắt mất cả cụm 81–120 ms: dataset giảm 47% (1019 → 538 khung) chỉ để độ trễ trung bình giảm từ 53 xuống 10 ms, một mức chính xác không cần thiết khi telemetry đến mỗi 77 ms. Nâng lên 1000 ms chỉ thêm 3 khung (384–884 ms), là các khung ở mép hai đoạn cảm biến mất tín hiệu: chúng mang tốc độ và góc lái đã cũ gần 1 giây, tức nhãn sai, nên không đáng giữ. 9 khung trễ trên 1 giây (trong đoạn mất tín hiệu) và 2 khung đầu mỗi drive (chưa có dòng telemetry nào trước đó, nên ASOF join không tìm được) luôn bị loại. Kết luận: 150 ms là lựa chọn hợp lý, vì nằm ngay trên cụm 120 ms. Mọi giá trị trong khoảng 121–383 ms đều cho cùng kết quả, nên dataset không nhạy với ngưỡng này, trừ khi ngưỡng nhỏ hơn chu kỳ telemetry.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Bất ngờ nhất: chia ngẫu nhiên theo khung làm rò rỉ cả 26/26 trip, và độ lệch đồng hồ chỉ 1,5 giây cũng đủ làm mọi khung của drive B ghép sai tốc độ và góc lái. Phần môi trường cũng tốn công trên Windows: file `.sh` bị đổi sang CRLF làm container Airflow không khởi động và làm `check_submission` tưởng file được bảo vệ đã bị sửa; Git Bash tự đổi đường dẫn `/opt/lab`.
Chưa rõ: nên chọn ngưỡng trùng thay đổi theo tốc độ thế nào cho tốt, và lấy mẫu theo quãng đường có tốt hơn theo thời gian không.
Lỗi pipeline chưa bắt được: **trôi đồng hồ** (clock drift). Pipeline chỉ trừ một độ lệch hằng số khai báo trong `sources.json`, và chỉ kiểm tra mẫu đầu tiên của B nằm trong 0,5 giây quanh giờ bắt đầu video. Nếu đồng hồ B trôi dần (1,5 s lúc đầu, 1,8 s lúc cuối) hoặc độ lệch khai báo sai vài trăm ms, `sync_gap_ms ≤ 150` vẫn đạt vì luôn có một dòng telemetry gần đó, nên mọi test đều qua dù khung ảnh ghép sai thời điểm. Cách bắt: ước lượng độ trễ thực tế từ dữ liệu, ví dụ tương quan chuyển động trong ảnh với tốc độ, rồi cảnh báo khi lệch với giá trị khai báo.
