#!/usr/bin/env python

import util
import werkzeug
from flask import Flask, Response, json
from flask_restplus import reqparse, Api, Resource, abort
from flask_restful import request
from config import settings
from util import MissingColumnException, InvalidDateFormatException
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

api = Api(app)
data = util.util()
data.start_log()

all_trees = data.s3_load_trees()
parser = reqparse.RequestParser()
parser.add_argument('address_to_score', type=werkzeug.datastructures.FileStorage, location='files')


@api.route('/project')
class project(Resource):

    @api.expect(parser)
    @api.response(200, 'Success')
    @api.response(400, 'Validation Error')
    def post(self):
        """
        Takes in an excel file of addresses and outputs a JSON with scores and rankings.
        """
        try:
            df, input_trees, needed_zones = data.parse_incoming_file(request)

        except MissingColumnException as e:
            abort(400, 'Excel File Missing Mandatory Column(s):', columns=str(e))

        except Exception as e:
            abort(400, str(e))

        project_trees = data.load_needed_trees(needed_zones, settings['directories']['current_tree_folder'])

        df = data.multiprocess_query(df, input_trees, project_trees)
        df = data.score_locations(df)
        df = data.rank_locations(df)
        df = data.replace_null(df)
        output_file = df.to_dict('index')
        resp = Response(json.dumps(output_file), mimetype='application/json')
        resp.status_code = 200

        return resp


@api.route('/project/health')
class projectHealth(Resource):

    @api.response(200, 'Success')
    def get(self):
        """
        Returns the status of the server if it's still running.
        """
        resp = Response(json.dumps('OK'), mimetype='application/json')
        resp.status_code = 200

        return resp


@api.route('/update/<string:date>', endpoint='update')
class UpdateData(Resource):

    @api.response(200, 'Success')
    @api.response(400, 'Input Error')
    def post(self, date=None):
        """
        Performs the updates to the tree data used to score the inputted addresses.
        """
        try:
            data.s3_load_trees(end_day=date)
        except InvalidDateFormatException:
            abort(400, 'Invalid Date format. Date has to be entered in yyyy-mm-dd format')

        resp = Response(json.dumps('Success'), mimetype='application/json')
        resp.status_code = 200

        return resp


# Actually setup the Api resource routing here
api.add_resource(project, '/project')
api.add_resource(projectHealth, '/project/health')
api.add_resource(UpdateData, '/update')


if __name__ == '__main__':
    app.run(debug=False)
