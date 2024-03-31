import luigi
import os
import tarfile
import gzip
from luigi import LocalTarget, Parameter, Task
import requests
import io
import pandas as pd

class DownloadDataset(Task):
    dataset_name = Parameter(default="GSE68849")

    def output(self):
        return LocalTarget(f'data/{self.dataset_name}/archive.tar')

    def run(self):
        download_url = f"https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset_name}&format=file"
        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        response = requests.get(download_url, stream=True)
        response.raise_for_status()
        with open(self.output().path, 'wb') as out_file:
            for chunk in response.iter_content(chunk_size=8192):
                out_file.write(chunk)


class ExtractFiles(Task):
    dataset_name = Parameter(default="GSE68849")

    def requires(self):
        return DownloadDataset(self.dataset_name)

    def output(self):
        # Поскольку создаем множество папок, определить точный output затруднительно
        # Вместо этого возвращаем общий путь как индикатор завершения
        return LocalTarget(f'data/{self.dataset_name}/extracted/')

    def run(self):
        extracted_path = self.input().path
        base_extracted_path = f'data/{self.dataset_name}/extracted/'
        os.makedirs(base_extracted_path, exist_ok=True)

        with tarfile.open(extracted_path, "r:*") as tar:
            for member in tar.getmembers():
                if member.isfile() and member.name.endswith('.gz'):
                    specific_folder = member.name.rsplit('.', 1)[0]  # Удаляем .gz из имени для создания папки
                    file_extracted_path = os.path.join(base_extracted_path, specific_folder)
                    os.makedirs(file_extracted_path, exist_ok=True)
                    tar.extract(member, file_extracted_path)

                    # Распаковка .gz файла
                    gz_file_path = os.path.join(file_extracted_path, member.name)
                    with gzip.open(gz_file_path, 'rb') as gz_file:
                        txt_file_path = gz_file_path.rsplit('.', 1)[
                            0]  # Удаляем .gz для получения пути текстового файла
                        with open(txt_file_path, 'wb') as txt_file:
                            txt_file.write(gz_file.read())

                    # Удаление .gz файла после распаковки
                    os.remove(gz_file_path)


class ProcessFiles(Task):
    dataset_name = Parameter(default="GSE68849")

    def requires(self):
        return ExtractFiles(self.dataset_name)

    def output(self):
        # Используем input как output, так как мы модифицируем файлы на месте
        return self.input()

    def run(self):
        extracted_path = self.input().path

        for root, dirs, files in os.walk(extracted_path):
            for dir_name in dirs:
                folder_path = os.path.join(root, dir_name)
                for file_name in os.listdir(folder_path):
                    if file_name.endswith('.txt'):
                        file_path = os.path.join(folder_path, file_name)

                        dfs = {}
                        with open(file_path) as f:
                            write_key = None
                            fio = io.StringIO()
                            for line in f.readlines():
                                if line.startswith('['):
                                    if write_key:
                                        fio.seek(0)
                                        dfs[write_key] = pd.read_csv(fio, sep='\t', header=0)
                                        fio = io.StringIO()
                                    write_key = line.strip('[]\n')
                                    continue
                                if write_key:
                                    fio.write(line)
                            if write_key:
                                fio.seek(0)
                                dfs[write_key] = pd.read_csv(fio, sep='\t', header=0)


                        for key, df in dfs.items():
                            section_file_path = os.path.join(folder_path, f"{file_name}_{key}.tsv")
                            df.to_csv(section_file_path, sep='\t', index=False)


class TrimProbesTable(Task):
    dataset_name = Parameter(default="GSE68849")

    def requires(self):
        return ProcessFiles(self.dataset_name)

    def run(self):
        base_path = self.input().path
        completion_flag_path = os.path.join(base_path, "trim_probes_table_done.txt")

        for root, dirs, files in os.walk(base_path):
            for file_name in files:
                if "probes" in file_name.lower() and file_name.endswith('.tsv'):
                    probes_file_path = os.path.join(root, file_name)
                    df_probes = pd.read_csv(probes_file_path, sep='\t')

                    columns_to_remove = ["Definition", "Ontology_Component", "Ontology_Process",
                                         "Ontology_Function", "Synonyms", "Obsolete_Probe_Id", "Probe_Sequence"]
                    trimmed_df_probes = df_probes.drop(columns=columns_to_remove, errors='ignore')

                    # Путь для сохранения не требует создания новых директорий, используем root
                    trimmed_probes_path = os.path.join(root, f"{os.path.splitext(file_name)[0]}_Trimmed.tsv")
                    trimmed_df_probes.to_csv(trimmed_probes_path, sep='\t', index=False)

        with open(completion_flag_path, 'w') as flag_file:
            flag_file.write('TrimProbesTable completed')

    def output(self):
        pass


if __name__ == '__main__':
    luigi.build([TrimProbesTable()], local_scheduler=True)
