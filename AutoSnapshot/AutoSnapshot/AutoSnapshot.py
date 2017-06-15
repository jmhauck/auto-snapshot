## Copyright 2017 Esri
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
## http://www.apache.org/licenses/LICENSE-2.0
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.​


###TODO read from webmap
###TODO handle multiple incident features
###TODO handle non-url based layers....keep in mind that non-url based layers don't support where based queries...only extent queries
###TODO handle cases where request or response are large
##TODO get popup and tags


import arcpy, urllib, json, datetime, time, logging, datetime, os, sys, traceback, gzip, email.generator, mimetypes, shutil, io

try:
    from urllib.request import urlopen as urlopen
    from urllib.request import Request as request
    from urllib.parse import urlencode as encode
    import configparser as configparser
    from io import StringIO
# py2
except ImportError:
    from urllib2 import urlopen as urlopen
    from urllib2 import Request as request
    from urllib import urlencode as encode
    import ConfigParser as configparser
    from cStringIO import StringIO

class _MultiPartForm(object):
    """Accumulate the data to be used when posting a form."""
    PY2 = sys.version_info[0] == 2
    PY3 = sys.version_info[0] == 3
    files = []
    form_fields = []
    boundary = None
    form_data = ""
    #----------------------------------------------------------------------
    def __init__(self, param_dict, files):
        self.boundary = None
        self.files = []
        self.form_data = ""
        if len(self.form_fields) > 0:
            self.form_fields = []

        if len(param_dict) == 0:
            self.form_fields = []
        else:
            for key, value in param_dict.items():
                self.form_fields.append((key, value))
                del key, value
        if len(files) == 0:
            self.files = []
        else:
            for key, value in files.items():
                self.add_file(fieldname=key,
                              filename=os.path.basename(value),
                              filepath=value,
                              mimetype=None)
        self.boundary = email.generator._make_boundary()
    #----------------------------------------------------------------------
    def get_content_type(self):
        """Gets the content type."""
        return 'multipart/form-data; boundary=%s' % self.boundary
    #----------------------------------------------------------------------
    def add_field(self, name, value):
        """Add a simple field to the form data."""
        self.form_fields.append((name, value))
    #----------------------------------------------------------------------
    def add_file(self, fieldname, filename, filepath, mimetype=None):
        """Add a file to be uploaded.
        Inputs:
           fieldname - name of the POST value
           fieldname - name of the file to pass to the server
           filePath - path to the local file on disk
           mimetype - MIME stands for Multipurpose Internet Mail Extensions.
             It's a way of identifying files on the Internet according to
             their nature and format. Default is None.
        """
        body = filepath
        if mimetype is None:
            mimetype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        self.files.append((fieldname, filename, mimetype, body))
    #----------------------------------------------------------------------
    @property
    def make_result(self):
        """Returns the data for the request."""
        if self.PY2:
            self._py2()
        elif self.PY3:
            self._py3()
        return self.form_data
    #----------------------------------------------------------------------
    def _py2(self):
        """python 2.x version of formatting body data"""
        boundary = self.boundary
        buf = StringIO()
        for (key, value) in self.form_fields:
            buf.write('--%s\r\n' % boundary)
            buf.write('Content-Disposition: form-data; name="%s"' % key)
            buf.write('\r\n\r\n%s\r\n' % value)
        for (key, filename, mimetype, filepath) in self.files:
            if os.path.isfile(filepath):
                buf.write('--{boundary}\r\n'
                          'Content-Disposition: form-data; name="{key}"; '
                          'filename="{filename}"\r\n'
                          'Content-Type: {content_type}\r\n\r\n'.format(
                              boundary=boundary,
                              key=key,
                              filename=filename,
                              content_type=mimetype))
                with open(filepath, "rb") as local_file:
                    shutil.copyfileobj(local_file, buf)
                buf.write('\r\n')
        buf.write('--' + boundary + '--\r\n\r\n')
        buf = buf.getvalue()
        self.form_data = buf
    #----------------------------------------------------------------------
    def _py3(self):
        """ python 3 method"""
        boundary = self.boundary
        buf = io.BytesIO()
        textwriter = io.TextIOWrapper(
            buf, 'utf8', newline='', write_through=True)

        for (key, value) in self.form_fields:
            textwriter.write(
                '--{boundary}\r\n'
                'Content-Disposition: form-data; name="{key}"\r\n\r\n'
                '{value}\r\n'.format(
                    boundary=boundary, key=key, value=value))
        for(key, filename, mimetype, filepath) in self.files:
            if os.path.isfile(filepath):
                textwriter.write(
                    '--{boundary}\r\n'
                    'Content-Disposition: form-data; name="{key}"; '
                    'filename="{filename}"\r\n'
                    'Content-Type: {content_type}\r\n\r\n'.format(
                        boundary=boundary, key=key, filename=filename,
                        content_type=mimetype))
                with open(filepath, "rb") as local_file:
                    shutil.copyfileobj(local_file, buf)
                textwriter.write('\r\n')
        textwriter.write('--{}--\r\n\r\n'.format(boundary))
        self.form_data = buf.getvalue()

class LayerData(object):
    def __init__(self, webmap_layer, item_info, item, time, visibleOnStartup, type):
        """Store key layer properties
        Keyword arguments:
        webmap_layer - operational layer from webmap
        item_info - 
        item - 
        time - time snapshot started
        visibleOnStartup - control if layer will be visible when added to the new webmap"""

        #item_info properties
        self.name = item_info['name'] + time
        self.drawingInfo = item_info['drawingInfo']      
        self.fields = item_info['fields']   
        self.description = item_info['description']       
        self.typeIdField = item_info['typeIdField']
        self.types = item_info['types']
        self.minScale = item_info['minScale']
        self.maxScale = item_info['maxScale']
        self.geometryType = item_info['geometryType']
        self.extent = item_info['extent']

        self.infoTemplate = None
        self.tags = None
        self.objectIdField = item['objectIdFieldName']
        
        #item properties
        self.graphics = item['features']

        #webmap_layer properties

        #additional properties
        self.visibleOnStartup = visibleOnStartup
        self.type = type

class Snapshot:
    def __init__(self):
        self._share_ids = []
        self._layers = []
        self._config_options = {}
        _time = datetime.datetime.utcnow()
        self.time = "{0}_{1}_{2}_({3}:{4} {5})".format(_time.month, _time.day, _time.year, _time.hour, _time.min, _time.second)

    #TODO clean up this business
    def _share_ids(self):
        return self._share_ids
    def time(self):
        return self.time
    def webmap_layers(self):
        return self.webmap_layers
    def _base_url(self):
        return self._portal_url + 'sharing/rest/'
    def _portal_url(self):
        return self._portal_url
    def _user_url(self):
        return '{0}/sharing/rest/content/users/{1}'.format(self._config_options['org_url'].rstrip('/'), self._config_options['username'])
    def _items_url(self):
        return '{0}/sharing/rest/content/items/'.format(self._config_options['org_url'].rstrip('/')) + "{0}"
    def _data_url(self):
        return '{0}/sharing/rest/content/items/'.format(self._config_options['org_url'].rstrip('/')) + "{0}/Data"

    def create_snapshot(self, config_file):
        """Snapshot"""
        try:         
            self._read_config(config_file)
            self._config_options['token'] = self._get_token()
            self._init_webmap(self._config_options['webmap_id'])
            self._create_folder()
            self._create_layers()
            self._create_map()
            self._add_layers_to_map()
            self._share_items(self._share_ids)
        except Exception:
            self._log_error()
        finally:
            self._end_logging()

    def _init_webmap(self, id):
        url = self._data_url().format(id)
        request_parameters = {'f' : 'json', 'token' : self._config_options['token']}
        item = self._url_request(url, request_parameters, error_text='Unable to find item with ID: {}'.format(id))
        self.web_map = item
        self.web_map['itemId'] = id
        self.webmap_layers = item['operationalLayers']

    def _create_folder(self):
        """create folder and add itemID to self._share_ids"""
        self._log_message('Creating Folder: ' + self._config_options['folder_name'] + "_" + self.time)

        url = self._user_url() + '/createFolder'
        request_parameters = {
            'f' : 'json', 
            'token' : self._config_options['token'], 
            'name': self._config_options['folder_name'] + "_" + self.time,
            'title': self._config_options['folder_name'] + "_" + self.time,
            'description': self._config_options['folder_description']
            }

        item = self._url_request(url, request_parameters, request_type='POST')

        self.folder_id = item['folder']['id']
        self._share_ids.append(self.folder_id)
        self._log_message('Folder created')

    def _create_layers(self):
        self._log_message('Creating Layers...')

        #get the incident features from the incident layer
        incident_item = self._get_incident_item()
        features = incident_item.graphics
        incident_type = incident_item.geometryType

        #create incident layer
        self._create_layer(incident_item, None, None, 'incident')

        #create a layer for each of the user defined layer ids
        analysis_layers = self._get_analysis_layers()
        for layer in analysis_layers:
            self._create_layer(layer, features, incident_type, 'analysis')
            
        print('layers created')
        self._log_message('Layers created')

    def _create_layer(self, layer, features, incident_type, type):
        #create layer and add itemId to self._share_ids

        if type == 'analysis':
            layer_items = self._get_layer_features(layer, features, incident_type)
        else:
            layer_items = [layer]

        url = self._user_url() + '/' + self.folder_id + '/addItem'

        for layer in layer_items:
            layer_definition = self._get_layer_definition(layer)

            item = self._url_request(url, layer_definition, 'POST')
            if item['success']:
                self._share_ids.append(item['id'])

    def _get_layer_definition(self, layer):
        return {
          'token' : self._config_options['token'],
          'title': layer.name,
          'type': 'Feature Collection',
          'tags': 'Snapshot',
          'description': None,
          'extent': layer.extent,
          'name': layer.name,
          'text': json.dumps({
            'layers': [{
              'layerDefinition': {
                'name': layer.name,
                'geometryType': layer.geometryType,
                'objectIdField': layer.objectIdField,
                'typeIdField': layer.typeIdField,
                'types': layer.types,
                'type': 'Feature Layer',
                'extent': layer.extent,
                'drawingInfo': layer.drawingInfo,
                'fields': layer.fields,
                'minScale': layer.minScale,
                'maxScale': layer.maxScale
              },
              'popupInfo': None,
              'featureSet': {
                'features': layer.graphics,
                'geometryType': layer.geometryType
              }
            }]
          }),
          'f': 'json'
        }

    def _get_incident_item(self):
        # This will first check if an incidnet layer comes from the user defined webmap
        # Next it will check if the service url has been provided directly 
        # Finally it will check and fetch the layer by item id if that has been provided

        #TODO update for failover...for example values are specified but layers don't exist in the map
        if 'incident_layer_name' in self._config_options and 'webmap_id' in self._config_options:
            webmap_layer = self._get_webmap_layer(self._config_options['incident_layer_name'])
            url = webmap_layer['url']
        elif 'incident_service_url' in self._config_options:
            url = self._config_options['incident_service_url']
        elif 'incident_service_id' in self._config_options and 'incident_layer_name' in self._config_options:
            url = self._get_url_by_id(self._config_options['incident_service_id'], self._config_options['incident_layer_name'])

        request_parameters = {'f' : 'json', 'token' : self._config_options['token']}

        item_info = self._url_request(url, request_parameters)

        if 'incident_where' in self._config_options:
            request_parameters['where'] = self._config_options['incident_where']
        elif 'feature_ids' in self._config_options:
            request_parameters['objectids'] = self._config_options['feature_ids']
        else:
            #need where or IDs...or does nothing indicate use-all features??
            raise
        request_parameters['fields'] = '*'
        item = self._url_request(url + '/query', request_parameters)

        data = LayerData(None, item_info, item, self.time, True, 'incident')

        self._layers.append(data)
        return data

    def _get_webmap_layer(self, name):
        #TODO this would only work for url based layers...would need to check with the ID for
        # things like feature collections
        for layer in self.webmap_layers:
            if layer['title'] == name or layer['id'] == name:
                return layer

    def _get_url_by_id(self, id, name):
        url = self._items_url().format(id)
        request_parameters = {'f' : 'json', 'token' : self._config_options['token']}
        item = self._url_request(url, request_parameters, error_text='Unable to find item with ID: {}'.format(id))

        url = item['url']
        
        request_parameters = {'f' : 'json', 'token' : self._config_options['token']}
        item_info = self._url_request(url, request_parameters)

        id = 0
        for l in item_info['layers']:
            if l['name'] == self._config_options['incident_layer_name']:
                id = l['id']
                break

        ##get oid field name and other layer props 
        return url + "/" + str(id)

    def _get_layer_features(self, layer, features, incident_type):
        ##TODO need to understand how to know when the geom will exceed what I can send in a request

        items = []
        url = layer['url']

        request_parameters = {'f' : 'json', 'token' : self._config_options['token']}
        item_info = self._url_request(url, request_parameters)

        ##TODO this does not work with multiple geoms...need to figure out the best way to deal with that
        #create the geom query string
        query_string = json.dumps(features[0]['geometry'])

        request_parameters = {
            'f' : 'json', 
            'token' : self._config_options['token'],
            'geometryType': incident_type,
            'geometry': query_string
            }

        item = self._url_request(url + "/query", request_parameters)

        
        #webmap_layer, item_info, item, type, time, visibleOnStartup):

        data = LayerData(layer, item_info, item, self.time, False, 'analysis')

        self._layers.append(data)
        items.append(data)

        return items

    def _get_analysis_layers(self):
        # This will first check if an analysis layer comes from the user defined webmap
        # Next it will check if the service url has been provided directly 
        # Finally it will check and fetch the layer by item id if that has been provided
        names = []
        if 'layer_names' in self._config_options and 'webmap_id' in self._config_options and len(self._config_options['layer_names']) > 0:
            names = self._config_options['layer_names']
        elif 'layer_service_urls' in self._config_options and len(self._config_options['layer_service_urls']) > 0:
            names = self._config_options['layer_service_urls']
        elif 'layer_service_ids' in self._config_options and 'layer_service_ids' in self._config_options and len(self._config_options['layer_service_ids']) > 0:
            names = self._get_url_by_id(self._config_options['layer_service_ids'], self._config_options['layer_service_ids'])
             
        layers = []
        for name in names:
            webmap_layer = self._get_webmap_layer(name)
            layers.append(webmap_layer)

        if len(layers) > 0:
            return layers
        else:
            #need one or more analysis layers
            raise

    def _create_map(self):
        url = self._user_url() + '/' + self.folder_id + '/addItem'
        map_definition = self._get_map_definition()
        item = self._url_request(url, map_definition, 'POST')
        if item['success']:
            self._share_ids.append(item['id'])

        self._share_ids.append(id)

        print('map created')

    def _get_map_definition(self):
        basemap_layers = self.web_map['baseMap']['baseMapLayers']
        spatial_reference = self.web_map['baseMap']['spatialReference']
        _basemap_layers = []
        """
        for basemap_layer in basemap_layers:
            _basemap_layers.append({"id": basemap_layer.id,
                                    "layerType": basemap_layer.layerType, 
                                    "url": basemap_layer.url,
                                    "visibility": basemap_layer.visibility,
                                    "opacity": basemap_layer.opacity,
                                    "title": basemap_layer.title
                                    })
                                    """
        base_map = {"baseMapLayers": basemap_layers}

        operational_layers = []
        for operational_layer in self._layers:
            operational_layers.append({"id": operational_layer.id, 
                                       "layerType": "ArcGISFeatureLayer", 
                                       "visibility": operational_layer.visible, 
                                       "opacity": operational_layer.opacity, 
                                       "title": operational_layer.label, 
                                       "type": "Feature Collection", 
                                       "itemId": operational_layer.itemId
                                       })
        return {"title": title,
                "type": "Web Map",
                "item": title,
                "extent": ext1.xmin + "," + ext1.ymin + "," + ext1.xmax + "," + ext1.ymax,
                "text": json.dumps({"operationalLayers": operational_layers,
                                    "baseMap": base_map,
                                    "spatialReference": spatial_reference,
                                    "version": "2.4"
                                    }),
                "tags": "Snapshot",
                "wabType": "HTML"
                }

    def _share_items(self, share_options):
        #share items per user definition
        url = self._user_url + '/shareItems'
        request_parameters = {'f' : 'json', 
                              'token' : self._config_options['token'],             
                              'everyone': self._config_options['share_everyone'],
                              'org': self._config_options['share_org'],
                              'items': self._share_ids.join(),
                              'groups': self._config_options['share_groups'],
                              'confirmItemControl': this._validate_group_item_control(self._config_options['share_groups'])}

        item = self._url_request(url, request_parameters)
        print('items shared')

    def _validate_group_item_control(self, groups):
        #check each group for updateitemcontrol cabability
        _groupIds = groups.split(',');

        #var _groups = this.groups.filter(function (group) {
        #  var capabilities = group.capabilities || [];
        #  return _groupIds.indexOf(group.id) > -1 && capabilities.indexOf("updateitemcontrol") > -1;
        #});
        #return _groups.length > 0;

    def _read_config(self, config_file):
        """Read the config and set global variables used in the script.
        
        Keyword arguments:
        config_file - Path to the configuration file. 
        If None it will look for a file called auto_snapshot.cfg in the same directory as the executing script.
        """

        config = configparser.ConfigParser()
        if config_file is None:
            config_file = os.path.join(os.path.dirname(__file__), 'auto_snapshot.cfg')
        config.readfp(open(config_file))

        log_path = _validate_input(config, 'Log File', 'path', 'path', False)
        if log_path is not None:
            self._config_options['log_path'] = log_path

        is_verbose = _validate_input(config, 'Log File', 'isVerbose', 'bool', False)
        if is_verbose is not None:
            self._config_options['is_verbose'] = is_verbose

        self._start_logging()

        #Folder config options
        folder_name = _validate_input(config, 'Folder', 'folder_name', 'string', True)
        if folder_name is not None:
            self._config_options['folder_name'] = folder_name

        folder_description = _validate_input(config, 'Folder', 'folder_description', 'string', False)
        if folder_description is not None:
            self._config_options['folder_description'] = folder_description
        
        #Portal config options
        self._config_options['org_url'] = _validate_input(config, 'Portal', 'org_url', 'url', True)
        self._config_options['username'] = _validate_input(config, 'Portal', 'username', 'string', True)
        self._config_options['pw'] = _validate_input(config, 'Portal', 'pw', 'string', True)

        token_url = _validate_input(config, 'Portal', 'tokenURL', 'url', False)
        if token_url is not None:
            self._config_options['token_url'] = token_url

        #Incident config options
        incident_service_id = _validate_input(config, 'Incident', 'incident_service_id', 'string', False)
        if incident_service_id is not None:
            self._config_options['incident_service_id'] = incident_service_id

        feature_ids = _validate_input(config, 'Incident', 'feature_ids', 'string', False)
        if feature_ids is not None:
            self._config_options['feature_ids'] = feature_ids

        incident_where = _validate_input(config, 'Incident', 'incident_where', 'string', False)
        if incident_where is not None:
            self._config_options['incident_where'] = incident_where

        incident_layer_name = _validate_input(config, 'Incident', 'incident_layer_name', 'string', False)
        if incident_layer_name is not None:
            self._config_options['incident_layer_name'] = incident_layer_name

        #Layers config options
        sub_layer_names = _validate_input(config, 'Layers', 'sub_layer_names', 'dict', False)
        if sub_layer_names is not None:
            self._config_options['sub_layer_names'] = sub_layer_names

        layer_service_ids = _validate_input(config, 'Layers', 'layer_service_ids', 'string', False)
        if layer_service_ids is not None:
            self._config_options['layer_service_ids'] = layer_service_ids

        layer_service_urls = _validate_input(config, 'Layers', 'layer_service_urls', 'list', False)
        if layer_service_urls is not None:
            self._config_options['layer_service_urls'] = layer_service_urls

        #WebMap config options
        webmap_id = _validate_input(config, 'WebMap', 'webmap_id', 'string', False)
        if webmap_id is not None:
            self._config_options['webmap_id'] = webmap_id

            incident_layer_name = _validate_input(config, 'WebMap', 'incident_layer_name', 'string', False)
            if incident_layer_name is not None:
                self._config_options['incident_layer_name'] = incident_layer_name

            layer_names = _validate_input(config, 'WebMap', 'layer_names', 'list', False)
            if layer_names is not None:
                self._config_options['layer_names'] = layer_names

        #Share config options
        share_everyone = _validate_input(config, 'Share', 'share_everyone', 'string', False)
        if share_everyone is not None:
            self._config_options['share_everyone'] = share_everyone

        share_org = _validate_input(config, 'Share', 'share_org', 'string', False)
        if share_org is not None:
            self._config_options['share_org'] = share_org

        share_groups = _validate_input(config, 'Share', 'share_groups', 'string', False)
        if share_groups is not None:
            self._config_options['share_groups'] = share_groups

    def _start_logging(self):
        """If a log file is specified in the config,
        create it if it doesn't exist and write the start time of the run."""
        self._config_options['start_time'] = datetime.datetime.now()

        if 'log_path' in self._config_options:
            log_path = self._config_options['log_path']
            is_file = os.path.isfile(log_path)

            logfile_location = os.path.abspath(os.path.dirname(log_path))
            if not os.path.exists(logfile_location):
                os.makedirs(logfile_location)

            if is_file:
                path = log_path
            else:
                path = os.path.join(logfile_location, "OverwriteLog.txt")

            log_path = path
            log = open(path, "a")
            date = self._config_options['start_time'].strftime('%Y-%m-%d %H:%M:%S')
            log.write("----------------------------" + "\n")
            log.write("Snapshot started: " + str(date) + "\n")
            log.close()

    def _log_message(self, my_message, is_error=False):
        """Log a new message and print to the python output.

        Keyword arguments:
        my_message - the message to log
        is_error - indicates if the message is an error, used to log even when verbose logging is disabled
        """
        date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if 'log_path' in self._config_options and (('is_verbose' in self._config_options and self._config_options['is_verbose']) or is_error):
            log = open(self._config_options['log_path'], "a")
            log.write("     " + str(date) + " - " +my_message + "\n")
            log.close()
        print("     " + str(date) + " - " +my_message + "\n")

    def _end_logging(self):
        """If a log file is specified in the config write the elapsed time."""
        if 'log_path' in self._config_options:
            log = open(self._config_options['log_path'], "a")
            endtime = datetime.datetime.now()

            log.write("Elapsed Time: " + str(endtime - self._config_options['start_time']) + "\n")
            log.close()

    def _log_error(self):
        """Log an error message."""
        pymsg = "PYTHON ERRORS:\nTraceback info:\n{1}\nError Info:\n{0}".format(str(sys.exc_info()[1]), "".join(traceback.format_tb(sys.exc_info()[2])))
        self._log_message(pymsg, True)

    def _url_request(self, url, request_parameters, request_type='GET', files=None, repeat=0, error_text="Error", raise_on_failure=True):
        """Send a new request and format the json response.
        Keyword arguments:
        url - the url of the request
        request_parameters - a dictionay containing the name of the parameter and its correspoinsding value
        request_type - the type of request: 'GET', 'POST'
        files - the files to be uploaded
        repeat - the nuber of times to repeat the request in the case of a failure
        error_text - the message to log if an error is returned
        raise_on_failure - indicates if an exception should be raised if an error is returned and repeat is 0"""
        if files is not None:
            mpf = _MultiPartForm(param_dict=request_parameters, files=files)
            req = request(url)
            body = mpf.make_result
            req.add_header('Content-type', mpf.get_content_type())
            req.add_header('Content-length', len(body))
            req.data = body
        elif request_type == 'GET':
            req = request('?'.join((url, encode(request_parameters))))
        else:
            headers = {'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',}
            req = request(url, encode(request_parameters).encode('UTF-8'), headers)

        req.add_header('Accept-encoding', 'gzip')

        response = urlopen(req)

        if response.info().get('Content-Encoding') == 'gzip':
            buf = io.BytesIO(response.read())
            with gzip.GzipFile(fileobj=buf) as gzip_file:
                response_bytes = gzip_file.read()
        else:
            response_bytes = response.read()

        response_text = response_bytes.decode('UTF-8')
        response_json = json.loads(response_text)

        if "error" in response_json:
            if repeat == 0:
                if raise_on_failure:
                    raise Exception("{0}: {1}".format(error_text, response_json))
                return response_json

            repeat -= 1
            time.sleep(2)
            response_json = self._url_request(
                url, request_parameters, request_type, files, repeat, error_text)

        return response_json

    def _get_token(self):
        """Returns a token for the given user and organization."""
        query_dict = {'username': self._config_options['username'],
                      'password': self._config_options['pw'],
                      'expiration': '60',
                      'referer': self._config_options['org_url'],
                      'f': 'json'}

        token_url = "https://www.arcgis.com/sharing/rest/generateToken"
        if 'token_url' in self._config_options:
            token_url = self._config_options['token_url']

        token_response = self._url_request(token_url, query_dict, 'POST', repeat=2, raise_on_failure=False)

        if "token" not in token_response:
            raise Exception("Unable to connect to specified portal. Please verify you are passing in your correct portal url, token url, username and password.")
        else:
            return token_response['token']

    def _wait_on_job(self, item_id, job_type, job_id, error_text):
        """Waits for a job to complete, if it fails an exception is raised.

        Keyword arguments:
        item_id - the id of the item to get the status for
        job_type - the type of job currently processing
        job_id - the id of the pending job
        error_text - the error to raise if the job fails"""
        url = '{0}sharing/rest/content/users/{1}/items/{2}/status'.format(self._config_options['org_url'], self._config_options['username'], item_id)
        parameters = {'token': self._config_options['token'], 'f': 'json', 'jobType' : job_type, 'jobId' : job_id}

        status = "processing"
        while status != "completed":
            response = self._url_request(url, parameters, repeat=2, error_text=error_text)
            status = response['status'].lower()
            if status == 'failed':
                raise Exception("{0}: {1}".format(error_text, str(response['statusMessage'])))
            elif status == "completed":
                break
            time.sleep(2)

def _validate_input(config, group, name, variable_type, required):
    """Validates and returns the correspoinding value defined in the config.

    Keyword arguments:
    config - the instance of the configparser
    group - the name of the group containing the property
    name - the name of the property to get that value for
    variable_type - the type of property, 'path', 'mapping' 'bool', otherwise return the raw string
    required - if the option is required and none is found than raise an exception
    """
    try:
        value = config.get(group, name)
        if value == '':
            raise configparser.NoOptionError(name, group)

        if variable_type == 'path':
            return os.path.normpath(value)
        elif variable_type == 'mapping':
            return list(v.split(',') for v in value.split(';'))
        elif variable_type == 'bool':
            return value.lower() == 'true'
        elif variable_type == 'dict':
            return json.loads(value)
        elif variable_type == 'list':
            return list(value.split(','))
        else:
            return value
    except (configparser.NoSectionError, configparser.NoOptionError):
        if required:
            raise
        elif variable_type == 'bool':
            return False
        else:
            return None

def run(config_file=None):
    """Create Snapshot."""
    snapshot = Snapshot()
    snapshot.create_snapshot(config_file)

if __name__ == "__main__":
    CONFIG_FILE = None
    if len(sys.argv) > 1:
        CONFIG_FILE = sys.argv[1]
    run(CONFIG_FILE)
