

#include <iostream>
#include <cassert>
#include <memory>
#include <parquet/api/reader.h>
#include <parquet/types.h>

#include <chrono>


void cpp_parquet_reader(){

    const char PARQUET_FILENAME[] = "catalog_sales.parquet";

    std::vector<int32_t> int32_values;
    std::vector<double> double_values;

    try {

      std::unique_ptr<parquet::ParquetFileReader> parquet_reader = parquet::ParquetFileReader::OpenFile(PARQUET_FILENAME, false);

      std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();

      int num_row_groups = file_metadata->num_row_groups();
      assert(num_row_groups == 2);

      int num_columns = file_metadata->num_columns();
      assert(num_columns == 34);

      for (int r = 0; r < num_row_groups; ++r) {

        std::shared_ptr<parquet::RowGroupReader> row_group_reader = parquet_reader->RowGroup(r);

        int64_t values_read = 0;
        int64_t rows_read = 0;
        int16_t definition_level;
        int16_t repetition_level;

        for (int col_id = 17; col_id < 18; ++col_id) {

          std::shared_ptr<parquet::ColumnReader> column_reader = row_group_reader->Column(col_id);

          parquet::Type::type column_type = column_reader->descr()->physical_type();

          if (column_type == parquet::Type::INT32) {
            parquet::Int32Reader* int32_reader = static_cast<parquet::Int32Reader*>(column_reader.get());
            int32_t value;
            while (int32_reader->HasNext()) {
              try {
                rows_read = int32_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
                int32_values.push_back(value);
              } catch (const std::exception& e) {
                break;
              }
              
            }
          } else if (column_type == parquet::Type::DOUBLE) {
            parquet::DoubleReader* double_reader = static_cast<parquet::DoubleReader*>(column_reader.get());
            double value;
            while (double_reader->HasNext()) {
              try {
                rows_read = double_reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
                double_values.push_back(value);
              } catch (const std::exception& e) {
                break;
              }
              
            }
          }
        }
      }
    } catch (const std::exception& e) {
      std::cerr << "Parquet read error: " << e.what() << std::endl;
    }


}


int main(int argc, char** argv){
    cpp_parquet_reader();
}