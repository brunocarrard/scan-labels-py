from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity, create_refresh_token
from controllers.login import Login
from controllers.verification import Verification
from controllers.getters import Getters
from controllers.picking import Picking
from controllers.delivery_line import DeliveryLines
import datetime

# import logging
# logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
app.config['JWT_SECRET_KEY'] = 'a@m!8r$eV$P5VXRd*7EF'
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = datetime.timedelta(minutes=30)
app.config['JWT_REFRESH_TOKEN_EXPIRES'] = datetime.timedelta(days=5)
jwt = JWTManager(app)
CORS(app)

@app.route('/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    valid = Login.verify_login(username, password)
    if valid['authenticated']:
        isah_user_dic = Login.get_isah_user(valid['windows_user'])
        access_token = create_access_token(isah_user_dic['isah_user'])
        refresh_token = create_refresh_token(isah_user_dic['isah_user'])
        if not isah_user_dic['found']:
            return jsonify({"error": isah_user_dic['message']}), 400
        else:
            return jsonify(access_token=access_token, refresh_token=refresh_token, isah_user=isah_user_dic['isah_user'])
    else:
        return jsonify({"error": valid['message']}), 400

# @app.route('/stockmovement')
# @jwt_required()
# def stockmovement():
    

@app.route('/refresh')
@jwt_required(refresh=True)
def refresh():
    current_user = get_jwt_identity()
    new_access_token = create_access_token(identity=current_user)
    return jsonify(access_token=new_access_token)

@app.route('/')
@jwt_required()
def data():
    user_input = request.args.get('value', type=str)  # Get user input from query parameter
    if not user_input:
        return jsonify({"error": "Must inform Sales Order"}), 400
    results = Getters.get_del_lines(user_input) 
    if len(results) == 0:
        return jsonify({"error": "Could not find Sales Order"}), 404
    results = Picking.build_picking_list(results)
    if len(results["parts"]) == 0:
        return jsonify({"error": "Order is ready and authorized"}), 404
    sub_parts = Getters.get_sub_parts(user_input)
    if sub_parts is not None:
        results["parts"] = results["parts"] + sub_parts
    certificates_lookup = Getters.get_available_certificates(user_input)

    for part in results["parts"]:
        part_code = part["PartCode"]
        if part_code in certificates_lookup:
            part["available_certificates"] = certificates_lookup[part_code]
    # if results["CustId"] == "AMZUS":
    #     Picking.VerifyAMZUSOrder(results)
    return jsonify(results)

@app.route('/', methods=['POST'])
@jwt_required()
def handle_post():
    isah_user = get_jwt_identity()
    if request.is_json:
        data = request.get_json()
        del_lines = data['delLines']
        del_lines = Verification.verify_v1(del_lines)
        lotnr_result = Verification.verify_lotnr(del_line["lotNr"] for del_line in del_lines)
        certificate_result = Verification.verify_certificate(del_line["certificate"] for del_line in del_lines)
        if not lotnr_result["valid"]:
            lotnrs = ", ".join(lotnr_result["invalid_results"])
            return jsonify({"error": f"LotNr {lotnrs} does not exists"}), 400
        if not certificate_result["valid"]:
            certificates = ", ".join(certificate_result["invalid_results"])
            return jsonify({"error": f"Certificate {certificates} does not exists"}), 400
        old_del_lines = Getters.get_del_lines(data['ordNr'])
        
        for line in old_del_lines:
            line['PartCode'] = line['PartCode'].strip()
        print(del_lines)
        import_del_lines = Picking.assembly_del_lines_with_scan_sales_parts([line for line in del_lines if line['SubPartInd'] == 0], old_del_lines)

        Picking.assembly_del_lines_with_scan_production_bom([line for line in del_lines if line['SubPartInd'] == 1], data['ordNr'], isah_user)
        DeliveryLines.create_new_lines(import_del_lines)
        DeliveryLines.authorize(import_del_lines)
        return ("Scans were imported.")
    else:
        return jsonify({"error": "Request must be JSON"}), 400

@app.route('/warehouses')
@jwt_required()
def warehouses():
    warehouseList = Getters.get_warehouses()
    return jsonify(warehouseList)

@app.route('/stock/parts')
@jwt_required()
def inventory_parts():
    warehouse = request.args.get('warehouse', type=str)  # Get user input from query parameter
    if not warehouse:
        return jsonify({"error": "Must select Warehouse"}), 400
    partsList = Getters.get_inventory_parts(warehouse)
    print(partsList)
    return jsonify(partsList)

if __name__ == '__main__':                                                                          
    app.run()