# AIOS Batch Tool

#### Môi trường
- Java JDK 17
- MySQL

#### Cài đặt
- Tạo database và thực hiện run script trong file *src/main/resources/init.sql* để tạo bảng.
- Thay thông tin DB tương tứng tại file *src/main/java/net/khaibq/addon/utils/DatabaseConnection.java*
- Thay các thông tin liên quan tới api, đường dẫn folder tại *src/main/java/net/khaibq/addon/utils/Constants.java*

#### Hướng dẫn chạy
Chạy lần lượt hàm main trong các file sau.
- File *src/main/java/net/khaibq/addon/AddonMaster.java* để thực hiện call api và lưu dữ liệu vào các bảng master
- File *src/main/java/net/khaibq/addon/AddonReadFile.java* để thực hiện đọc dữ liệu Virtual Machine và lưu vào DB
- File *src/main/java/net/khaibq/addon/AddonVirtualMachine.java* để thực hiện tính toán đưa ra dữ liệu phần Virtual Machine

Với các phần Disk, Redhat, Window sẽ được hướng dẫn sau.

Sau khi việc test hoàn tất sẽ tiến hành tách thành các file jar để chạy cron job sau.
