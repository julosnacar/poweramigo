import json
import pymysql
#valores de configuracion
endpoint = 'amigodb.cnmk2mw6km6u.ap-southeast-2.rds.amazonaws.com'
username = 'amigo'
password = 'tn4lgMza(1jRm7GoEp0:'
database_name = 'amigodb'
#conectar
connection = pymysql.connect(host=endpoint, user=username, passwd=password, db=database_name)

allocations_path = '/allocations'
role_path = '/role'
roles_path = '/roles'
allocation_path = '/allocation'
installations_path = '/installations'
installation_path = '/installation'
devices_path = '/devices'
device_path = '/device'
users_path = '/users'
user_path = '/user'

def lambda_handler(event, context):
    print('Request event: ', event)
    response = None
    try:
        http_method =  event.get("requestContext")["http"]["method"]
        path = event.get("requestContext")["http"]["path"]

        if http_method == 'GET' and path == allocations_path:
            cursor = connection.cursor()
            cursor.execute('SELECT * from t_allocations')
            rows = cursor.fetchall()
            res = []
            for row in rows:
                res.append("{0} {1} {2} {3}".format(row[0],row[1],row[2],row[3]))
            response = build_response(200, res)
        elif http_method == 'GET' and path == allocation_path:
            cursor = connection.cursor()
            cursor.execute('SELECT * from t_allocations where allocation_id ='+event.get("queryStringParameters")['id'])
            rows = cursor.fetchall()
            response = build_response(200,rows)
        elif http_method == 'POST' and path == allocation_path:
            cursor = connection.cursor()
            columna = event.get('queryStringParameters')
            id = columna.pop('id')
            for col in columna:
                cursor.execute('update t_allocations set %s = %s where allocation_id = %s'%(col,columna[col],id))
                connection.commit()
            response = build_response(200, 'Datos actualizados correctamente')
        elif http_method == 'PUT' and path == allocation_path:
            cursor = connection.cursor()
            cursor.execute('insert into t_allocations values (%s,%s,%s,%s,%s,%s)'%(event.get("queryStringParameters")['id'],event.get("queryStringParameters")['installation'],event.get("queryStringParameters")['user'],event.get("queryStringParameters")['created'],event.get("queryStringParameters")['updated'],event.get("queryStringParameters")['deleted']))
            connection.commit()
            response = build_response(200, 'Datos creados correctamente')
        elif http_method == 'DELETE' and path == allocation_path:
            cursor = connection.cursor()
            cursor.execute('update t_allocations set deleted_at = now() where allocation_id = %s'%(event.get("queryStringParameters")['id']))
            connection.commit()
            response = build_response(200, 'Datos borrados')
##TABLA roles 
        elif http_method == 'GET' and path == roles_path:
            cursor = connection.cursor()
            cursor.execute('select * from t_roles')
            rows = cursor.fetchall()
            res = []
            for row in rows:
                res.append("{0} {1} {2} {3} {4} {5} {6}".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6]))
            response = build_response(200, res)
        elif http_method == 'GET' and path == role_path:
            cursor = connection.cursor()
            cursor.execute('select * from t_roles where role_id = '+event.get('queryStringParameters')['id'])
            rows = cursor.fetchall()
            response = build_response(200,rows)
        elif http_method == 'POST' and path == role_path:
            cursor = connection.cursor()
            columna = event.get('queryStringParameters')
            id = columna.pop('id')
            for col in columna:
                cursor.execute('update t_roles set %s = %s where role_id = %s'%(col,columna[col],id))
                connection.commit()
            response = build_response(200,'Datos actualizados')
        elif http_method == 'PUT' and path == role_path:
            cursor = connection.cursor()
            id = event.get('queryStringParameters')['id']
            app_id = event.get('queryStringParameters')['app_id']
            name = event.get('queryStringParameters')['name']
            status = event.get('queryStringParameters')['status']
            c = event.get('queryStringParameters')['created_at']
            u = event.get('queryStringParameters')['updated_at']
            d = event.get('queryStringParameters')['deleted_at']
            cursor.execute('insert into t_roles values (%s,%s,%s,%s,%s,%s,%s)'%(id,app_id,name,status,c,u,d))
            connection.commit()
            response = build_response(200,'Datos insertados')
        elif http_method == 'DELETE' and path == role_path:
            cursor = connection.cursor()
            id = event.get('queryStringParameters')['id']
            cursor.execute('update t_roles set deleted_at = now() where role_id = %s'%(id))
            connection.commit()
            response = build_response(200,'Datos Borrados')
##TABLA INSTALLATION
        elif http_method == 'GET' and path == installations_path:
            cursor = connection.cursor()
            cursor.execute('select * from t_installations')
            rows = cursor.fetchall()
            res = []
            for row in rows:
                res.append("{0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10} {11} {12} {13} {14} {15} {16} {17} {18} {19} {20} {21} {22} {23} {24} {25}".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],row[19],row[20],row[21],row[22],row[23],row[24],row[25]))
            response = build_response(200, res)
        elif http_method == 'GET' and path == installation_path:
            cursor = connection.cursor()
            id = event.get('queryStringParameters')['id']
            cursor.execute('select * from t_installations where installation_id=%s'%(id))
            rows = cursor.fetchall()
            res = []
            for row in rows:
                res.append("{0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10} {11} {12} {13} {14} {15} {16} {17} {18} {19} {20} {21} {22} {23} {24} {25}".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],row[19],row[20],row[21],row[22],row[23],row[24],row[25]))
            response = build_response(200, res)
        elif http_method == 'DELETE' and path == installation_path:
            cursor = connection.cursor()
            cursor.execute('update t_installations set deleted_at = now() where installation_id='+event.get('queryStringParameters')['id'])
            connection.commit()
            response = build_response(200,'Datos Borrados')
        elif http_method == 'PUT' and path == installation_path:
            cursor = connection.cursor()
            id = event.get('queryStringParameters')['id']
            app=event.get('queryStringParameters')['app']
            name=event.get('queryStringParameters')['name']
            address=event.get('queryStringParameters')['address']
            postcode=event.get('queryStringParameters')['postcode']
            suburb=event.get('queryStringParameters')['suburb']
            state=event.get('queryStringParameters')['state']
            country=event.get('queryStringParameters')['country']
            longitude=event.get('queryStringParameters')['longitude']
            latitude=event.get('queryStringParameters')['latitude']
            size=event.get('queryStringParameters')['size']
            power=event.get('queryStringParameters')['power']
            image=event.get('queryStringParameters')['image']
            logo=event.get('queryStringParameters')['logo']
            ct=event.get('queryStringParameters')['ct']
            status=event.get('queryStringParameters')['status']
            ttype=event.get('queryStringParameters')['ttype']
            triphasic=event.get('queryStringParameters')['triphasic']
            parent = event.get('queryStringParameters')['parent']
            I_id=event.get('queryStringParameters')['I_id']
            deleted=event.get('queryStringParameters')['deleted']
            customer=event.get('queryStringParameters')['customer']
            avatar=event.get('queryStringParameters')['avatar']
            created=event.get('queryStringParameters')['created']
            updated=event.get('queryStringParameters')['updated']
            stop=event.get('queryStringParameters')['stop']
            cursor.execute('insert into t_installations values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'%(id,app,name,address,postcode,suburb,state,country,longitude,latitude,size,power,image,logo,ct,status,ttype,triphasic,parent,I_id,deleted,customer,avatar,created,updated,stop))
            connection.commit()
            response = build_response(200,'Datos Insertados')
        elif http_method == 'POST' and path == installation_path:
            cursor = connection.cursor()
            columna = event.get('queryStringParameters')
            id = columna.pop('id')
            for col in columna:
                cursor.execute('update t_installations set %s = %s where installation_id = %s'%(col,columna[col],id))
                connection.commit()
            response = build_response(200,'datos actualizados')
##Tabla device
        elif http_method == 'PUT' and path == device_path:
            cursor = connection.cursor()
            id = event.get('queryStringParameters')['id']
            app = event.get('queryStringParameters')['app_id']
            installation = event.get('queryStringParameters')['installation_id']
            user = event.get('queryStringParameters')['user_id']
            mac = event.get('queryStringParameters')['mac']
            role = event.get('queryStringParameters')['role']
            created = event.get('queryStringParameters')['created_at']
            updated = event.get('queryStringParameters')['updated_at']
            deleted = event.get('queryStringParameters')['deleted_at']
            alerted = event.get('queryStringParameters')['alerted_at']
            cursor.execute('insert into t_devices values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'%(id,app,installation,user,mac,role,created,updated,deleted,alerted))
            connection.commit()
            response = build_response(200,'Datos insertados')
        elif http_method == 'POST' and path == device_path:
            cursor = connection.cursor()
            columna = event.get('queryStringParameters')
            id = columna.pop('id')
            for col in columna:
                cursor.execute('update t_devices set %s = %s where device_id = %s'%(col,columna[col],id))
                connection.commit()
            response = build_response(200,'datos actualizados')
            
        elif http_method == 'DELETE' and path == device_path:
            cursor = connection.cursor()
            cursor.execute('update t_devices set deleted_at = now() where device_id ='+event.get('queryStringParameters')['id'])
            connection.commit()
            response = build_response(200,'Borrado')
            
        elif http_method == 'GET' and path == device_path:
            cursor = connection.cursor()
            cursor.execute('select * from t_devices where device_id = '+event.get('queryStringParameters')['id'])
            rows = cursor.fetchall()
            res = []
            for row in rows:
                res.append("{0} {1} {2} {3} {4} {5} {6} {7} {8} {9}".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]))
            response = build_response(200, res)
        elif http_method == 'GET' and path == devices_path:
            cursor = connection.cursor()
            cursor.execute('select * from t_devices')
            rows = cursor.fetchall()
            res = []
            for row in rows:
                res.append("{0} {1} {2} {3} {4} {5} {6} {7} {8} {9}".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]))
            response = build_response(200, res)
##Tabla User
        elif http_method == 'PUT' and path == user_path:
            cursor = connection.cursor()
            user_id = event.get('queryStringParameters')['user_id']
            app_id = event.get('queryStringParameters')['app_id']
            parent_id = event.get('queryStringParameters')['parent_id']
            role_id = event.get('queryStringParameters')['role_id']
            avatar_id = event.get('queryStringParameters')['avatar_id']
            token_id = event.get('queryStringParameters')['token_id']
            first_name = event.get('queryStringParameters')['first_name']
            last_name = event.get('queryStringParameters')['last_name']
            email = event.get('queryStringParameters')['email']
            password = event.get('queryStringParameters')['password']
            mobile_num = event.get('queryStringParameters')['mobile_num']
            status = event.get('queryStringParameters')['status']
            registered_at = event.get('queryStringParameters')['registered_at']
            api_token = event.get('queryStringParameters')['api_token']
            change_password = event.get('queryStringParameters')['change_password']
            remember_token = event.get('queryStringParameters')['remember_token']
            deleted_at = event.get('queryStringParameters')['deleted_at']
            cursor.execute('insert into t_users values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'%(user_id,app_id,parent_id,role_id,avatar_id,token_id,first_name,last_name,email,password,mobile_num,status,registered_at,api_token,change_password,remember_token,deleted_at))
            connection.commit()
            response = build_response(200,'Datos insertados')
        
        elif http_method == 'POST' and path == user_path:
            cursor = connection.cursor()
            columna = event.get('queryStringParameters')
            id = columna.pop('id')
            for col in columna:
                cursor.execute('update t_users set %s = %s where user_id = %s'%(col,columna[col],id))
                connection.commit()
            response = build_response(200,'datos actualizados')
        
        elif http_method == 'DELETE' and path == user_path:
            cursor = connection.cursor()
            cursor.execute('update t_users set deleted_at = now() where user_id ='+event.get('queryStringParameters')['id'])
            connection.commit()
            response = build_response(200,'Borrado')
        
        elif http_method == 'GET' and path == user_path:
            cursor = connection.cursor()
            cursor.execute('select * from t_users where user_id = '+event.get('queryStringParameters')['id'])
            rows = cursor.fetchall()
            res = []
            for row in rows:
                res.append("{0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10} {11} {12} {13} {14} {15} {16}".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16]))
            response = build_response(200, res)
            
        elif http_method == 'GET' and path == users_path:
            cursor = connection.cursor()
            cursor.execute('select * from t_users')
            rows = cursor.fetchall()
            res = []
            for row in rows:
                res.append("{0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10} {11} {12} {13} {14} {15} {16}".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16]))
            response = build_response(200, res)
        else:
            response = build_response(404, event)

    except Exception as e:
        print('Error:', e)
        response = build_response(400, event)
   
    return response

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            # Check if it's an int or a float
            if obj % 1 == 0:
                return int(obj)
            else:
                return float(obj)
        # Let the base class default method raise the TypeError
        return super(DecimalEncoder, self).default(obj)

def build_response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps(body, cls=DecimalEncoder)
    }
