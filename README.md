# PBI-BDA
Kumpulan query yang digunakan untuk preprocessing data dan analisis data dalam Project Based Internship oleh Rakamin dan Kimia Farma (sebagai Big Data Analyst)

# 1. Import data ke Google BigQuery :
### Final Transaction (`kf_final_transaction`),
### Kantor Cabang (`kf_kantor_cabang`),
### Product (`kf_product`),
### Inventory (`kf_inventory`)

# 2. Mencari informasi awal untuk setiap data

## melihat jumlah baris data

SELECT COUNT(*) FROM `kimia_farma.kf_final_transaction`;
SELECT COUNT(*) FROM `kimia_farma.kf_kantor_cabang`;
SELECT COUNT(*) FROM `kimia_farma.kf_product`;
SELECT COUNT(*) FROM `kimia_farma.kf_inventory`;

## melihat tipe data setiap kolom pada setiap tabel khusus di tabel `kf_final_transaction`, tipe data kolom `date` diubah format penulisannyake YYYY-MM-DD, agar bisa dihitung jumlah baris data nya, dan melakukan operasi lainnya

CREATE OR REPLACE TABLE `kimia_farma.kf_final_transaction_converted` AS
SELECT
  transaction_id,
  PARSE_DATE('%m/%d/%Y', `date`) AS `date`,
  branch_id,
  customer_name,
  product_id,
  price,
  discount_percentage,
  rating
FROM
  `kimia_farma.kf_final_transaction`;

### Informasi awal `kf_final_transaction`

#### Cek tipe data

#### Jumlah baris data

SELECT COUNT(*) FROM `kimia_farma.kf_final_transaction_converted`;

#### Jumlah customer_name, cabang, dan product unik yang terdapat di transaksi

SELECT COUNT(DISTINCT customer_name) FROM `kimia_farma.kf_final_transaction_converted`;
SELECT COUNT(DISTINCT branch_id) FROM `kimia_farma.kf_final_transaction_converted`;
SELECT COUNT(DISTINCT product_id) FROM `kimia_farma.kf_final_transaction_converted`;

#### Range data

SELECT MIN(price) FROM `kimia_farma.kf_final_transaction_converted`;
SELECT MAX(price) FROM `kimia_farma.kf_final_transaction_converted`;

SELECT MIN(discount_percentage) FROM `kimia_farma.kf_final_transaction_converted`;
SELECT MAX(discount_percentage) FROM `kimia_farma.kf_final_transaction_converted`;

SELECT MIN(rating) FROM `kimia_farma.kf_final_transaction_converted`;
SELECT MAX(rating) FROM `kimia_farma.kf_final_transaction_converted`;

##### Rata-rata nilai transaksi dan rating transaksi

SELECT AVG(price) FROM `kimia_farma.kf_final_transaction_converted`;
SELECT AVG(rating) FROM `kimia_farma.kf_final_transaction_converted`;

#### Modus rating, branch_id, product_id

-- rating
SELECT
    rating,
    COUNT(*) AS jml_rating_unik
FROM
    `kimia_farma.kf_final_transaction_converted`
GROUP BY
    rating
ORDER BY
    jml_rating_unik DESC
LIMIT 1;

-- branch_id
SELECT
    branch_id,
    COUNT(*) AS jml_cabang_unik
FROM
    `kimia_farma.kf_final_transaction_converted`
GROUP BY
    branch_id
ORDER BY
    jml_cabang_unik DESC
LIMIT 1;

-- product
SELECT
    product_id,
    COUNT(*) AS jml_product_unik
FROM
    `kimia_farma.kf_final_transaction_converted`
GROUP BY
    product_id
ORDER BY
    jml_product_unik DESC
LIMIT 1;

### Informasi awal `kf_kantor_cabang`

#### Jumlah Baris Data (Header Tidak Terhitung)

SELECT COUNT(*) FROM `kimia_farma.kf_kantor_cabang`;

#### Jumlah unik branch_id, branch_category, branch_name, kota, provinsi

SELECT COUNT(DISTINCT branch_id) FROM  `kimia_farma.kf_kantor_cabang`;
SELECT COUNT(DISTINCT branch_category) FROM  `kimia_farma.kf_kantor_cabang`;
SELECT COUNT(DISTINCT branch_name) FROM  `kimia_farma.kf_kantor_cabang`;
SELECT COUNT(DISTINCT kota) FROM  `kimia_farma.kf_kantor_cabang`;
SELECT COUNT(DISTINCT provinsi) FROM  `kimia_farma.kf_kantor_cabang`;

#### Rata-rata rating cabang

SELECT AVG(rating)  FROM  `kimia_farma.kf_kantor_cabang`;

### Informasi awal `kf_product`

#### Jumlah baris Data (Header Tidak Terhitung)

SELECT COUNT(*) FROM `kimia_farma.kf_product`;

#### Jumlah unik product_id, product_name, product_category

SELECT COUNT (DISTINCT product_id) FROM `kimia_farma.kf_product`;
SELECT COUNT (DISTINCT product_name) FROM `kimia_farma.kf_product`;
SELECT COUNT (DISTINCT product_category) FROM `kimia_farma.kf_product`;

#### Range price

SELECT MIN(price) FROM `kimia_farma.kf_product`;
SELECT MAX(price) FROM `kimia_farma.kf_product`;

### Informasi awal `kf_inventory`

#### Jumlah Baris Data (Header Tidak Dihitung)

SELECT COUNT(*) FROM `kimia_farma.kf_inventory`;

#### Jumlah unik inventory_id, branch_id, product_id, product_name

SELECT COUNT(DISTINCT inventory_id) FROM `kimia_farma.kf_inventory`;
SELECT COUNT(DISTINCT branch_id) FROM `kimia_farma.kf_inventory`;
SELECT COUNT(DISTINCT product_id) FROM `kimia_farma.kf_inventory`;
SELECT COUNT(DISTINCT product_name) FROM `kimia_farma.kf_inventory`;

#### Range opname_stock

SELECT MIN(opname_stock) FROM `kimia_farma.kf_inventory`;
SELECT MAX(opname_stock) FROM `kimia_farma.kf_inventory`;

# 3. Membuat `tabel_analisa`

## `tabel_analisa` terdiri dari 13 kolom gabungan dari keempat tabel, serta 4 kolom tambahan (tahun, persentase_gross_laba, nett_sales, nett_profit) 
### `tahun` : tahun transaksi dilakukan
### `persentase_gross_laba` : persentase laba yang seharusnya diterima dari obat
### `nett_sales` : harga setelah diskon (actual_price x (1-discount_percentage))
### `nett_profit` : keuntungan yang diperoleh Kimia Farma (nett_sales x persentase_gross_laba)

CREATE OR REPLACE TABLE `rakamin-kf-analytics-460003.kimia_farma.tabel_analisa` AS

WITH transaksi_dan_margin AS (
  SELECT
    ftc.transaction_id,
    CAST(EXTRACT(YEAR FROM DATE(`date`))AS STRING) AS tahun,
    `date`,
    ftc.branch_id,
    kc.branch_name,
    kc.kota,
    kc.provinsi,
    kc.rating AS rating_cabang,
    ftc.customer_name,
    ftc.product_id,
    p.product_name,
    ftc.price AS actual_price,
    ftc.discount_percentage,

    -- Hitung margin laba (dalam persen)
    CASE
      WHEN ftc.price <= 50000 THEN 0.10
      WHEN ftc.price > 50000 AND ftc.price <= 100000 THEN 0.15
      WHEN ftc.price > 100000 AND ftc.price <= 300000 THEN 0.20
      WHEN ftc.price > 300000 AND ftc.price <= 500000 THEN 0.25
      WHEN ftc.price > 500000 THEN 0.30
      ELSE 0
    END AS persentase_gross_laba,

    -- Harga setelah diskon
    ROUND(ftc.price * (1 - ftc.discount_percentage), 0) AS nett_sales,

    ftc.rating AS rating_transaksi

  FROM
    `rakamin-kf-analytics-460003.kimia_farma.kf_final_transaction_converted` AS ftc
  LEFT JOIN
    `rakamin-kf-analytics-460003.kimia_farma.kf_product` AS p
    ON ftc.product_id = p.product_id
  LEFT JOIN
    `rakamin-kf-analytics-460003.kimia_farma.kf_kantor_cabang` AS kc
    ON ftc.branch_id = kc.branch_id
)

SELECT
  transaction_id,
  tahun,
  `date`,
  branch_id,
  branch_name,
  kota,
  provinsi,
  rating_cabang,
  customer_name,
  product_id,
  product_name,
  actual_price,
  discount_percentage,
  persentase_gross_laba,
  nett_sales,

  -- Hitung net_profit dari alias kolom yang sudah ada
  ROUND(nett_sales * persentase_gross_laba, 0) AS nett_profit,

  rating_transaksi

FROM transaksi_dan_margin;

# 4. Exploratory Data Analysis `tabel_analysis`

## cek jumlah baris data `tabel_analisa`

SELECT COUNT(*) FROM `kimia_farma.tabel_analisa`;

## cek jumlah baris data yang setidaknya mengandung 1 nilai NULL

SELECT COUNT(*) AS jumlah_baris_null
FROM `rakamin-kf-analytics-460003.kimia_farma.tabel_analisa`
WHERE 
  transaction_id IS NULL OR
  tahun IS NULL OR
  `date` IS NULL OR
  branch_id IS NULL OR
  branch_name IS NULL OR
  kota IS NULL OR
  provinsi IS NULL OR
  rating_cabang IS NULL OR
  customer_name IS NULL OR
  product_id IS NULL OR
  product_name IS NULL OR
  actual_price IS NULL OR
  discount_percentage IS NULL OR
  persentase_gross_laba IS NULL OR
  nett_sales IS NULL OR
  nett_profit IS NULL OR
  rating_transaksi IS NULL;

## Menghapus data NULL

CREATE OR REPLACE TABLE `rakamin-kf-analytics-460003.kimia_farma.tabel_analisa` AS
SELECT *
FROM `rakamin-kf-analytics-460003.kimia_farma.tabel_analisa`
WHERE 
  transaction_id IS NOT NULL AND
  tahun IS NOT NULL AND
  `date` IS NOT NULL AND
  branch_id IS NOT NULL AND
  branch_name IS NOT NULL AND
  kota IS NOT NULL AND
  provinsi IS NOT NULL AND
  rating_cabang IS NOT NULL AND
  customer_name IS NOT NULL AND
  product_id IS NOT NULL AND
  product_name IS NOT NULL AND
  actual_price IS NOT NULL AND
  discount_percentage IS NOT NULL AND
  persentase_gross_laba IS NOT NULL AND
  nett_sales IS NOT NULL AND
  nett_profit IS NOT NULL AND
  rating_transaksi IS NOT NULL;

## cek kembali jumlah baris data `tabel_analisa` setelah dilakukan penghapusan data

SELECT COUNT(*) FROM `kimia_farma.tabel_analisa`;

### NOTE : kondisi data pada `tabel_analisa` milik saya awalnya ada 672460 baris data, sementara jumlah baris data pada tabel `kf_final_transaction` ada 672458 baris data. Setelah dicek, ternyata di `tabel_analisa` saya ada 2 baris data yang NULL, sehingga dilakukan penghapusan

## cek tipe data untuk setiap kolom

### setelah dicek, tipe data kolom `persentase_gross_laba` dan `nett_profit` adalah FLOAT, sehingga dilakukan update tipe data menjadi NUMERIC agar data bisa digunakan nantinya di Google Looker Studio

CREATE OR REPLACE TABLE `rakamin-kf-analytics-460003.kimia_farma.tabel_analisa` AS
SELECT
  transaction_id,
  tahun,
  `date`,
  branch_id,
  branch_name,
  kota,
  provinsi,
  rating_cabang,
  customer_name,
  product_id,
  product_name,
  actual_price,
  discount_percentage,
  CAST (persentase_gross_laba AS NUMERIC) AS persentase_gross_laba,
  nett_sales,
  CAST(nett_profit AS NUMERIC) AS nett_profit,
  rating_transaksi
FROM
  `rakamin-kf-analytics-460003.kimia_farma.tabel_analisa`;

## menampilkan `tabel_analisa`

SELECT * FROM `kimia_farma.tabel_analisa`
ORDER BY `date` ASC;

# 5. Lanjutan Kebutuhan Analisis

## cek jumlah cabang di setiap provinsi

SELECT
    provinsi,
    COUNT(branch_id) AS jumlah_cabang
FROM
    `kimia_farma.kf_kantor_cabang`
GROUP BY
    provinsi
ORDER BY
    jumlah_cabang DESC;
