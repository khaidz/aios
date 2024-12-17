# AIOS Batch Tool

#### Môi trường
- Java JDK 17
- MySQL

#### Cài đặt
- Tạo database và thực hiện run script trong file *src/main/resources/init.sql* để tạo bảng.
- Thay thông tin DB tương tứng tại file *src/main/java/net/khaibq/addon/utils/DatabaseConnection.java*
- Thay các thông tin liên quan tới api, đường dẫn folder tại *src/main/java/net/khaibq/addon/utils/Constants.java*
- Các thông tin folder sẽ là đường dẫn để lưu kết quả tính toán/ thư mục để lấy dữ liệu đầu vào

#### Hướng dẫn chạy
Chạy lần lượt hàm main trong các file sau.
- File *src/main/java/net/khaibq/addon/AiosMaster.java* để thực hiện call api và lưu dữ liệu vào các bảng master
- File *src/main/java/net/khaibq/addon/AiosBatchApp.java* để thực hiện tính toán VM, Disk, Redhat, Window

Sau khi việc test hoàn tất sẽ tiến hành tách thành các file jar để chạy cron job sau.
