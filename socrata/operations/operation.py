class Operation(object):
    def __init__(self, publish, **kwargs):
        self.publish = publish
        self.properties = kwargs

    def csv(self, file, *args, **kwargs):
        """
        Create a revision on that view, then upload a file of
        type CSV and wait for validation to complete. Returns
        an `OutputSchema` which, when applied, will by applied
        to the view.
        """
        return self.run(file, lambda upload: upload.csv(file), *args, **kwargs)

    def xls(self, file, *args, **kwargs):
        """
        Create a revision on that view, then upload a file of
        type XLS and wait for validation to complete. Returns
        an `OutputSchema` which, when applied, will by applied
        to the view.
        """
        return self.run(file, lambda upload: upload.xls(file), *args, **kwargs)

    def xlsx(self, file, *args, **kwargs):
        """
        Create a revision on that view, then upload a file of
        type XLSX and wait for validation to complete. Returns
        an `OutputSchema` which, when applied, will by applied
        to the view.
        """
        return self.run(file, lambda upload: upload.xlsx(file), *args, **kwargs)

    def tsv(self, file, *args, **kwargs):
        """
        Create a revision on that view, then upload a file of
        type TSV and wait for validation to complete. Returns
        an `OutputSchema` which, when applied, will by applied
        to the view.
        """
        return self.run(file, lambda upload: upload.tsv(file), *args, **kwargs)

    def df(self, dataframe, filename="Dataframe", *args, **kwargs):
        """
        Create a revision on that view, then upload the contents
        of a pandas dataframe and wait for validation to complete.
        Returns an `OutputSchema` which, when applied, will by
        applied to the view.
        """
        return self.run(dataframe, lambda upload: upload.df(dataframe), filename, *args, **kwargs)


    def shapefile(self, file, *args, **kwargs):
        """
        Create a revision on that view, then upload the contents
        of the shapefile and wait for validation to complete.
        Returns an `OutputSchema` which, when applied, will by
        applied to the view.
        """
        return self.run(file, lambda upload: upload.shapefile(file), *args, **kwargs)


    def kml(self, file, *args, **kwargs):
        """
        Create a revision on that view, then upload the contents
        of the kml and wait for validation to complete.
        Returns an `OutputSchema` which, when applied, will by
        applied to the view.
        """
        return self.run(file, lambda upload: upload.kml(file), *args, **kwargs)

    def geojson(self, file, *args, **kwargs):
        """
        Create a revision on that view, then upload the contents
        of the geojson and wait for validation to complete.
        Returns an `OutputSchema` which, when applied, will by
        applied to the view.
        """
        return self.run(file, lambda upload: upload.geojson(file), *args, **kwargs)
