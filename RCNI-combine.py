import pandas as pd
import os


class IngestRCNIMixin:

    def __init__(self, directory, drop_recordcode=False, files=None, name_filter=""):
        '''
        Mixin file for combining RCNI files from a directory

        param directory[string]: Directory location RCNI files are located
        param files[string]: List of files to be ingested
        param name_filter[string]: String matching criteria for filtering files within the directory
        '''
        if directory[-1] == "/":
            self.directory = directory
        else:
            self.directory = f"{directory}/"
        self.drop_recordcode = drop_recordcode
        self.files = files
        self.name_filter = name_filter

    def toPandasDF(self, **load_params):
        # generates a Pandas dataframe from the RCNI files in the directory

        # load the first RCNI file into a dataframe
        df = pd.read_csv(f"{self.directory}{self.files[0]}", **load_params)

        # iterate over the remaining RCNI files and union them to the dataframe
        for file in self.files[1:]:
            df_temp = pd.read_csv(f"{self.directory}{file}", **load_params)
            df = pd.concat([df, df_temp])

        if self.drop_recordcode:
            df = df[df.Recordcode==1].drop(['Recordcode'], axis=1)
        return df

    def toSparkDF(self, sc, **load_params):
        return sc.createDataFrame(self.toPandasDF(**load_params))

    def _files_list(self, directory):
        # locates all files in a given directory and returns a list
        # of all file names
        pass

    def _filter_names(self, files, name_filter):
        # filters the list of files in the directory to only include
        # files with a name that starts with the name_filter string

        filtered_files = list()
        name_length = len(name_filter)
        for file in files:
            if file[:name_length] == name_filter:
                filtered_files.append(file)
        return filtered_files

    def files_from_dir(self):
        # generates a list of all files within a given directory that match the name filtering criteria
        # and sets the self.files attribute to this list

        files = self._files_list(self.directory)
        filtered_files = self._filter_names(files, self.name_filter)
        self.files = filtered_files
        return self


class IngestRCNILocally(IngestRCNIMixin):

    def _files_list(self, directory):
        return os.listdir(directory)


if __name__ == "__main__":
    df = IngestRCNILocally("C:/Data/RCNI-1").files_from_dir().toPandasDF()
    df.to_csv("C:/Data/RCNI-1-combined.csv", index=False)

