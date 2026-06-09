# Hướng Dẫn Thiết Kế Superset Dashboard Dành Cho TheLook Lakehouse

Dự án TheLook Lakehouse sở hữu 2 điểm mạnh cốt lõi cần được phô diễn trên Dashboard:
1. **Real-time CDC Streaming**: Dữ liệu bắn từ Postgres lên Dashboard với độ trễ cực thấp (nhờ Kafka + Spark 30s micro-batch).
2. **Medallion Architecture (dbt)**: Dữ liệu thô được làm sạch, biến đổi và liên kết thành chuẩn Star Schema ở tầng `mart`, tối ưu hóa cho Trino query tốc độ cao.

Dưới đây là các bước chi tiết để xây dựng một Dashboard "Executive" thể hiện đúng đẳng cấp của hệ thống.

---

## Bước 1: Khai báo Datasets (Tầng Mart)

Đăng nhập vào Superset (mặc định: `http://localhost:8088` với tài khoản `admin`/`admin123`).
Vào mục **Data** -> **Datasets** -> **+ Dataset** và thêm các bảng sau từ Database `Trino`, Schema `mart`:

- `fct_orders`: Thông tin tổng quan đơn hàng
- `fct_events`: Tracking hành vi người dùng (click, view, add to cart...)
- `fct_order_items`: Thông tin chi tiết từng sản phẩm trong đơn hàng
- `dim_products`: Thông tin cấu hình sản phẩm
- `dim_customers`: Thông tin nhân khẩu học của khách hàng

---

## Bước 2: Khởi tạo các nhóm Biểu đồ (Charts) chiến lược

### Nhóm 1: Thể hiện sức mạnh Real-time (Streaming)
Mục đích: Cho thấy số liệu nhảy liên tục mỗi vài chục giây mà không cần batch-processing qua đêm.

1. **Big Number - Total Revenue Today**
   - **Dataset**: `fct_order_items`
   - **Metric**: `SUM(sale_price)`
   - **Filter**: Thời gian lọc trong ngày hôm nay (`Today`).

2. **Time-series Line Chart - Live Traffic & Orders**
   - **Dataset**: `fct_events` (hoặc `fct_orders`)
   - **Time Grain**: `Minute` (Từng phút)
   - **Metric**: `COUNT(*)` (Số lượng truy cập / Số lượng đơn hàng mới).

### Nhóm 2: Thể hiện năng lực biến đổi dữ liệu sâu (dbt Transform)
Mục đích: Khẳng định dbt đã xử lý logic gom nhóm session và hành vi người dùng rất mượt mà.

3. **Funnel Chart - Conversion Funnel (Phễu chuyển đổi)**
   - **Dataset**: `fct_events`
   - **Group by**: `event_type`
   - **Metric**: `COUNT(DISTINCT session_id)`
   - *Cách hoạt động*: Vẽ phễu hành vi từ `Home` -> `Product` -> `Cart` -> `Purchase`.

4. **Pie Chart - Orders by Traffic Source**
   - **Dataset**: `fct_orders`
   - **Group by**: `traffic_source` (Organic, Facebook, Google, Email...)
   - **Metric**: `COUNT(order_id)`

### Nhóm 3: Phân tích nghiệp vụ cốt lõi (Business Intelligence)
Mục đích: Chứng minh dữ liệu đã được join chuẩn (Star schema) và dọn dẹp sạch sẽ.

5. **Horizontal Bar Chart - Top 10 Best Selling Products**
   - **Dataset**: Dataset ảo (SQL Lab) join `fct_order_items` và `dim_products`
   - **Metric**: `SUM(sale_price)` và `COUNT(order_id)`
   - **Limit**: 10
   - **Sort**: Descending `SUM(sale_price)`.

6. **Bar Chart - Customers Demographics**
   - **Dataset**: `dim_customers`
   - **Group by**: `gender` và nhóm độ tuổi (`age_group`)
   - **Metric**: `COUNT(*)`

---

## Bước 3: Hướng Dẫn Sắp Xếp Bố Cục Dashboard (Grid Layout)

Để Dashboard nhìn chuyên nghiệp, khoa học và đúng chuẩn UI/UX phân tích dữ liệu (theo nguyên tắc đọc từ trên xuống dưới, từ trái sang phải), bạn nên sắp xếp các Chart theo dạng lưới (Grid) như sau:

### 🟢 Hàng 1: Top-level KPIs (Các chỉ số quan trọng nhất)
*Nên đặt trên cùng để sếp nhìn vào là biết ngay tình hình kinh doanh tổng quan.*
- **Big Number - Total Revenue** (Doanh thu tổng) - [Chiều rộng: 1/4 màn hình]
- **Big Number - Total Orders** (Tổng đơn hàng) - [Chiều rộng: 1/4 màn hình]
- **Big Number - Active Users** (Người dùng hoạt động) - [Chiều rộng: 1/4 màn hình]
- **Big Number - Conversion Rate** (Tỷ lệ chuyển đổi) - [Chiều rộng: 1/4 màn hình]

### 🔵 Hàng 2: Xu Hướng Theo Thời Gian (Trends)
*Biểu đồ đường cần không gian rộng để hiển thị thời gian rõ ràng, không bị rối mắt.*
- **Time-series Line Chart (Live Traffic & Orders)** - [Chiều rộng: Full 100% màn hình]
  - *Mẹo:* Để chiều cao vừa phải (khoảng 300px - 400px) để không đẩy các biểu đồ khác xuống quá sâu.

### 🟡 Hàng 3: Hành Vi & Nguồn Khách Hàng (Acquisition & Behavior)
*Chia đôi màn hình để so sánh 2 góc nhìn về khách hàng.*
- **Funnel Chart (Conversion Funnel)** - [Chiều rộng: 50% bên trái]
  - *Lý do:* Nằm bên trái vì luồng người dùng (User Flow) thường được đọc từ trái sang.
- **Pie Chart (Orders by Traffic Source)** - [Chiều rộng: 50% bên phải]
  - *Lý do:* Cho biết nguồn khách hàng đến từ đâu lọt vào cái phễu bên trái.

### 🟣 Hàng 4: Chi Tiết Sản Phẩm & Nhân Khẩu Học (Details & Demographics)
*Chi tiết cụ thể đặt ở dưới cùng cho những ai muốn phân tích sâu hơn.*
- **Horizontal Bar Chart (Top 10 Best Selling Products)** - [Chiều rộng: 50% hoặc 60% bên trái]
  - *Lý do:* Cột nằm ngang cần bề rộng để hiển thị rõ tên sản phẩm dài.
- **Stacked Bar Chart (Customers Demographics)** - [Chiều rộng: 50% hoặc 40% bên phải]
  - *Lý do:* Tháp tuổi và giới tính thường không chiếm quá nhiều bề ngang.

### 💡 Mẹo Thao Tác Trực Tiếp Trên Superset

1. **Sử dụng Layout Elements (Bên phải màn hình Edit Dashboard):**
   - Kéo thẻ **Row** thả vào màn hình trống trước.
   - Kéo các biểu đồ thả vào trong từng Row để đảm bảo chúng không bị xô lệch khi thu phóng màn hình.
2. **Thêm Tiêu đề (Header):**
   - Kéo thẻ **Header** đặt ngay trên cùng, đặt tên là `TheLook Lakehouse - Realtime Executive Dashboard` để tăng độ uy tín.

---

## 🚀 TÍNH NĂNG CHỐT HẠ: Auto-Refresh (Quan trọng nhất)

Để chứng minh đây là một hệ thống **Live Lakehouse**:
1. Trong màn hình Dashboard, bấm vào dấu 3 chấm `...` ở góc phải.
2. Chọn **Set auto-refresh interval**.
3. Cài đặt thành **30 seconds** (hoặc 10s nếu muốn).

**Hiệu ứng**: Cứ mỗi 30s, biểu đồ sẽ tự chớp nhẹ và nhảy số mới do đằng sau Spark Streaming vừa nhồi xong 1 micro-batch data mới. Đây là chi tiết đắt giá nhất để tạo hiệu ứng "Wow" khi demo dự án!

### Nhóm 4: Phân tích Nâng cao (Advanced Analytics)
Mục đích: Phân tích dữ liệu theo nhiều chiều (multi-dimensional) để tìm ra các insight sâu hơn về doanh thu và khách hàng.

7. **Pivot Table v2 (Bảng chéo có màu Heatmap)**
   - **Mục đích**: Phân tích sự dịch chuyển Doanh thu của từng Ngành hàng qua các tháng.
   - **Dataset**: `fct_order_items` kết hợp `dim_products` (SQL Lab: Lưu thành View/Dataset)
   - **Cấu hình**:
     - **Rows**: `category` (hoặc `department`)
     - **Columns**: `created_at` (chọn grain là `Month` hoặc `Year`)
     - **Metrics**: `SUM(sale_price)`
     - Bật tính năng **Conditional Formatting** để tạo hiệu ứng bôi màu (Heatmap) nổi bật các giá trị cao/thấp.

8. **Big Number with Trendline (Số lớn kèm biến động)**
   - **Mục đích**: Hiện thị Doanh thu tháng này và biến động qua từng ngày.
   - **Dataset**: `fct_order_items`
   - **Cấu hình**:
     - **Metrics**: `SUM(sale_price)`
     - **Time Column**: `created_at`
     - **Time Grain**: `Day`
     - *Hiệu ứng*: Hiển thị một con số tổng cực lớn phía trên và một đường line biểu diễn biến động lên xuống ở dưới rất trực quan.

9. **Sunburst Chart (Biểu đồ quạt đa tầng)**
   - **Mục đích**: Xem tỷ trọng doanh thu đi từ tầng Ngành hàng -> Danh mục -> Thương hiệu.
   - **Dataset**: Dataset kết hợp `fct_order_items` và `dim_products`
   - **Cấu hình**:
     - **Hierarchy**: Kéo lần lượt các cột theo thứ tự: `department` ➔ `category` ➔ `brand`.
     - **Metrics**: `SUM(sale_price)`
     - *Hiệu ứng*: Khi click vào một tầng ngoài cùng (VD: Men), biểu đồ sẽ tự động zoom vào các danh mục con bên trong.

10. **Grouped Bar Chart (Cột ghép nhóm)**
    - **Mục đích**: So sánh tương quan Doanh thu giữa Top các Quốc gia qua từng năm.
    - **Dataset**: Dataset kết hợp `fct_order_items` và `dim_customers`
    - **Cấu hình**:
      - **X-axis**: `created_at` (Year)
      - **Metrics**: `SUM(sale_price)`
      - **Dimensions (Group by)**: `country`
      - **Bar mode**: Chọn `Grouped` (Các cột sẽ đứng cạnh nhau thay vì xếp chồng lên nhau).

---

## Bước 4: Tạo "Master Dataset" (Virtual Dataset) trong SQL Lab

Để làm được các biểu đồ phân tích chéo như Nhóm 4 (ví dụ: Xem doanh thu của Sản phẩm A ở Quốc gia B), anh cần một dataset gom đủ các bảng. Thay vì viết code dbt thêm một bảng to đùng, anh có thể dùng tính năng Virtual Dataset của Superset:

1. Trên menu trên cùng, chọn **SQL Lab** ➔ **SQL Editor**.
2. Chọn Database: `Trino`, Schema: `mart`.
3. Dán đoạn mã SQL sau vào ô Editor:
   ```sql
   SELECT 
       i.order_item_id,
       i.order_id,
       i.item_status,
       i.sale_price,
       i.revenue,
       i.item_created_at,
       -- Thông tin sản phẩm (Dim Products)
       p.product_category AS category,
       p.product_department AS department,
       p.product_brand AS brand,
       p.cost,
       -- Thông tin khách hàng (Dim Customers)
       c.country,
       c.gender,
       c.age,
       c.customer_tier
   FROM mart.fct_order_items i
   LEFT JOIN mart.dim_products p ON i.product_id = p.product_id
   LEFT JOIN mart.dim_customers c ON i.user_id = c.user_id
   ```
4. Bấm **Run** để chạy thử.
5. Khi có kết quả, bấm nút **Save** ➔ Chọn **Save as Dataset**.
6. Đặt tên Dataset là: `Master_Order_Items_Analytics`.

Bây giờ khi tạo biểu đồ Pivot Table hoặc Sunburst, anh chỉ cần chọn nguồn dữ liệu là `Master_Order_Items_Analytics`. Nó sẽ có đầy đủ cột từ `sale_price` cho tới `country`, `category` để anh thoả sức kéo thả!

---
