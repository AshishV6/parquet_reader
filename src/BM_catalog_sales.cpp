
#include <iostream>
#include <cassert>
#include <memory>
#include <parquet/api/reader.h>
#include <parquet/types.h>

#include <benchmark/benchmark.h>

#include <chrono>


void cpp_parquet_reader(){

    const char PARQUET_FILENAME[] = "catalog_sales_standard.parquet";

    std::vector<int32_t> int32_values;
    std::vector<double> double_values;

    try {

      std::unique_ptr<parquet::ParquetFileReader> parquet_reader = parquet::ParquetFileReader::OpenFile(PARQUET_FILENAME, false);

      std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();

      int num_row_groups = file_metadata->num_row_groups();
    //   assert(num_row_groups == 2);

      int num_columns = file_metadata->num_columns();
    //   assert(num_columns == 34);

      for (int r = 0; r < num_row_groups; ++r) {

        std::shared_ptr<parquet::RowGroupReader> row_group_reader = parquet_reader->RowGroup(r);

        int64_t values_read = 0;
        int64_t rows_read = 0;
        std::vector<int16_t> definition_levels;
        std::vector<int16_t> repetition_levels;

        for (int col_id = 0; col_id < 16; col_id+=15) {

          std::shared_ptr<parquet::ColumnReader> column_reader = row_group_reader->Column(col_id);

          parquet::Type::type column_type = column_reader->descr()->physical_type();

          if (column_type == parquet::Type::INT32) {
            parquet::Int32Reader* int32_reader = static_cast<parquet::Int32Reader*>(column_reader.get());
            std::vector<int32_t> batch_values(8192);
            while (int32_reader->HasNext()) {
              try {
                rows_read = int32_reader->ReadBatch(8192, definition_levels.data(), repetition_levels.data(), batch_values.data(), &values_read);
                int32_values.insert(int32_values.end(), batch_values.begin(), batch_values.begin() + rows_read);
              } catch (const std::exception& e) {
                break;
              }
              
            }
          } else if (column_type == parquet::Type::DOUBLE) {
            parquet::DoubleReader* double_reader = static_cast<parquet::DoubleReader*>(column_reader.get());
            std::vector<double> batch_values(8192);
            while (double_reader->HasNext()) {
              try {
                rows_read = double_reader->ReadBatch(8192, definition_levels.data(), repetition_levels.data(), batch_values.data(), &values_read);
                double_values.insert(double_values.end(), batch_values.begin(), batch_values.begin() + rows_read);
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


static void BM_parquet_reader(benchmark::State& state) {
  for (auto _ : state){
    cpp_parquet_reader();
  }
}


BENCHMARK(BM_parquet_reader);
                      

int main()
{
    int argc = 1;
    char arg0_default[] = "benchmark";
    char *args_default = arg0_default;
    char **argv = &args_default;
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();

    return 0;
}




// BENCHMARK_MAIN();



// int main(int argc, char** argv) {                                     
//     char arg0_default[] = "benchmark";                                  
//     char* args_default = arg0_default;                                  
//     if (!argv) {                                                        
//       argc = 1;                                                         
//       argv = &args_default;                                             
//     }                                                                   
//     ::benchmark::Initialize(&argc, argv);                               
//     if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1; 
//     ::benchmark::RunSpecifiedBenchmarks();                              
//     ::benchmark::Shutdown();                                            
//     return 0;                                                           
//   }  





