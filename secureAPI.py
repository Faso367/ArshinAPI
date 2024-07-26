#from flask import Flask, request, jsonify
from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, VARCHAR, Boolean, SmallInteger, Date, create_engine, and_, or_,desc
from sqlalchemy.schema import ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, class_mapper
import logging, os
from flask_talisman import Talisman
from marshmallow import Schema, fields, validates, ValidationError, validate, error_store
from flask import Flask, request, jsonify, make_response, render_template, session, flash
import jwt
from datetime import datetime, timedelta
from functools import wraps
import bleach
import psycopg2

current_year =  datetime.now().year

#app = Flask(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'KEEP_IT_A_SECRET'
app.config['CLIENT_KEY'] = '123'

# Включаем принудительное использование HTTPS
#app.config['SESSION_COOKIE_SECURE'] = True

# Защищаем от атак CSP.
# Подробнее на https://superset-bi.ru/superset-3-talisman-security-considerations-csp-requirements/#%D0%9A%D0%BE%D0%BD%D1%84%D0%B8%D0%B3%D1%83%D1%80%D0%B0%D1%86%D0%B8%D1%8F_Talisman_%D0%BF%D0%BE_%D1%83%D0%BC%D0%BE%D0%BB%D1%87%D0%B0%D0%BD%D0%B8%D1%8E

# Настройка политики CSP
csp = {
    'default-src': "'none'",  # Запрещает выполнение любых скриптов по умолчанию
    'script-src': "'none'",   # Запрещает выполнение JavaScript !!!!!!!!!!!!!!!!!!!
    'style-src': "'none'",    # Запрещает загрузку стилей !!!!!!!!!!!!!!!!!!!!!
    'img-src': "'none'",      # Запрещает загрузку изображений
    'font-src': "'self'",     # Разрешает шрифты только с текущего домена
}


talisman = Talisman(app)

#app.config['DEBUG'] = True
#app.config['ENV'] = 'development'
engine = create_engine('postgresql://postgres:password@localhost:5432/Arshindb')
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

# ----------------------- Модели базы данных
class UniquePoveritelOrgs(Base):
	__tablename__ = 'UniquePoveritelOrgs'
	id = Column(BigInteger(), primary_key=True)
	poveritelOrg = Column(VARCHAR(256), unique=True)

class UniqueTypeNames(Base):
	__tablename__ = 'UniqueTypeNames'
	id = Column(BigInteger(), primary_key=True)
	typeName = Column(VARCHAR(512))

class UniqueRegisterNumbers(Base):
	__tablename__ = 'UniqueRegisterNumbers'
	id = Column(BigInteger(), primary_key=True)
	registerNumber = Column(VARCHAR(16))

class EquipmentInfoPartitioned(Base):
    __tablename__ = 'EquipmentInfoPartitioned'
    id = Column(BigInteger(), primary_key=True)
    serialNumber = Column(VARCHAR(256))
    svidetelstvoNumber = Column(VARCHAR(256))
    poverkaDate = Column(Date())
    konecDate = Column(Date())
    vri_id = Column(BigInteger())
    isPrigodno = Column(Boolean())
    poveritelOrgId = Column(Integer(), ForeignKey('UniquePoveritelOrgs.id'))
    typeNameId = Column(Integer(), ForeignKey('UniqueTypeNames.id'))
    registerNumberId = Column(Integer(), ForeignKey('UniqueRegisterNumbers.id'))
    year = Column(SmallInteger())

# Будем создавать партиции с такими полями
def create_base_attributes():
    return {
    'id': Column(BigInteger(), primary_key=True),
    'serialNumber': Column(VARCHAR(256)),
    'svidetelstvoNumber': Column(VARCHAR(256)),
    'poverkaDate': Column(Date()),
    'konecDate': Column(Date()),
    'vri_id': Column(BigInteger()),
    'isPrigodno': Column(Boolean()),
    'poveritelOrgId': Column(Integer(), ForeignKey('UniquePoveritelOrgs.id')),
    'typeNameId': Column(Integer(), ForeignKey('UniqueTypeNames.id')),
    'registerNumberId' : Column(Integer(), ForeignKey('UniqueRegisterNumbers.id')),
    'year': Column(SmallInteger())
    }

# Динамическое создание классов-моделей для партиций
for year in range(2019, current_year + 1):
    class_name = f'EquipmentInfo_{year}'
    tablename = f'EquipmentInfo_{year}'
    # Создаём словарь для полей класса. Так как tablename разный, то мы добавляли сначала его
    attributes = {'__tablename__': tablename}
    # Преобразуем остальные поля в пары и добавляем их
    attributes.update(create_base_attributes())
    # Создаём классы с указанным именем, входным параметром и полями
    globals()[class_name] = type(class_name, (Base,), attributes)



# Настройка логгера
logger = logging.getLogger('arshinAPIlogger')
logger.setLevel(logging.ERROR)  # Установите уровень логирования на DEBUG

# Определение пути к файлу логирования
script_directory = os.path.dirname(os.path.abspath(__file__))
log_file_path = os.path.join(script_directory, 'arshinAPI.log')

# Создание обработчика для записи в файл
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.ERROR)

# Установка формата для обработчиков
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(formatter)

# Добавление обработчиков к логгеру
logger.addHandler(file_handler)

def token_required(func):
    @wraps(func)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'message': 'Token is missing!'}), 401

        try:
            data = jwt.decode(token.split(" ")[1], app.config['SECRET_KEY'], algorithms=["HS256"])
        except Exception as e:
            return jsonify({'message': 'Invalid token', 'error': str(e)}), 403

        return func(*args, **kwargs)

    return decorated

@app.route('/login', methods=['POST'])
def login():
    auth = request.form
    if auth.get('key') == app.config['CLIENT_KEY']:
        token = jwt.encode({'user': auth.get('username'), 'exp': datetime.utcnow() + timedelta(minutes=50)}, app.config['SECRET_KEY'], algorithm='HS256')
        return jsonify({'token': token})
    return make_response('Unable to verify', 403, {'WWW-Authenticate': 'Basic realm="Login required!"'})


# @app.route('/hello', methods=['GET'])
# @token_required
# def hello():
#     return(request.args)


@app.errorhandler(400)
def bad_request(error):
    return jsonify({'error': 'Bad request'}), 400

@app.errorhandler(401)
def unauthorized(error):
    return jsonify({'error': 'Unauthorized'}), 401

@app.errorhandler(403)
def forbidden(error):
    return jsonify({'error': 'Forbidden'}), 403

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def server_error(error):
    return jsonify({'error': 'Internal server error'}), 500


impreciseSearchParams = ['year', 'rows', 'start', 'sort']
preciseSearchParams = ['poveritelOrg', 'registerNumber', 'typeName', 'serialNumber', 'svidetelstvoNumber', 'poverkaDate', 'konecDate', 'isPrigodno']
correctParams = ['vri_id', 'poveritelOrg', 'registerNumber', 'serialNumber', 'svidetelstvoNumber',
                 'poverkaDate', 'konecDate', 'typeName', 'isPrigodno',
                 'year', 'sort', 'start', 'rows', 'search']


def to_int_if_possible(s):
    if s.isdigit():
        return int(s)
    return s

def try_to_int(val):
    try:
        return int(val)
    except ValueError:
        logger.error('Не удалось конвертировать строку в целое число')
        return -1
        #return "Введите целое число"

class ParamsSchema(Schema):
    sort = fields.Str()
    vri_id = fields.Int()
    year = fields.Int(validate=validate.Range(min = 2019, max = current_year, error = f'Параметр year может принимать значения от 2019 до {current_year}'))
    rows = fields.Int(validate=validate.Range(min = 1, max = 100, error = 'Параметр rows может принимать значения от 1 до 100'))
    start = fields.Int(validate=validate.Range(min = 0, max = 99999, error = 'Параметр rows может принимать значения от 0 до 99999'))
    isPrigodno = fields.Str(validate=validate.OneOf(choices=['true', 'false'], error = 'Параметр isPrigodno может принимать значение true или false'))
    svidetelstvoNumber = fields.Str()
    registerNumber = fields.Str()
    serialNumber = fields.Str()
    poverkaDate = fields.Str() # !!!!!!!!!!!
    konecDate = fields.Str() # !!!!!!!!!!
    poveritelOrg = fields.Str()
    typeName = fields.Str()

    @validates('sort')
    def validate_sort(self, value):
        value = value.replace('%20', ' ').replace('+', ' ')
        parts = value.split(' ')

        if len(parts) != 2:
            raise ValidationError("Invalid format for sort parameter.")
        else:
            if parts[-1] not in ['asc', 'desc']:
                raise ValidationError("Order must be 'asc' or 'desc'.")

def sanitize_input(inputDict):
    res = dict()
    for k, v in inputDict.items():
        res[k] = bleach.clean(v)
    return res

def validation(paramsAndValues):

    params = paramsAndValues.keys()

    invalidParams = {key for key in params if key not in correctParams}

    if len(invalidParams) > 0:
        raise ValidationError(dict=invalidParams, message="Были найдены некорректные названия параметров")
        #return jsonify(Invalid_data=invalidParams, Error="Были найдены некорректные названия параметров")

    # Очищаем значения от потенциально опасных скриптов
    clearedDict = sanitize_input(paramsAndValues)
    # Валидируем значения параметров
    schema.load(clearedDict)       

    if len(set(paramsAndValues)) != len(paramsAndValues):
        raise ValidationError(dict=invalidParams, message="Некоторые параметры повторяются, используйте & для перечисления значений")
        #return jsonify(Invalid_data=invalidParams, Error="Некоторые параметры повторяются, используйте & для перечисления значений")

    # Если задан параметр search и ещё один из четких параметров
    elif len([key for key in params if key == 'search' or key in preciseSearchParams]) > 1:
        raise ValidationError(list=[key for key in params if key == 'search' or key in preciseSearchParams], message="Нельзя использовать поиск по одному параметру и по всем одновременно")
        #return jsonify(Invalid_data=[key for key in paramsDict.keys() if key == 'search' or key in preciseSearchParams], Error="Нельзя использовать поиск по одному параметру и по всем одновременно")
    
    else:
        return True
    
    
schema = ParamsSchema()
@app.route('/vri', methods=['GET'])
@token_required
def vri():
    '''Вызывается пользователем с заданными параметрами'''

    try:
        paramsAndValues = request.args.to_dict()

        if validation(paramsAndValues) == True:
            newparamsDict = dict()
            defaultValues = {'year': current_year, 'rows': [10], 'start': [0]}

            # Добавляем дефолтные пары ключ-значение, если их значения не заданы 
            for key, value in defaultValues.items():       
                if key not in paramsAndValues:
                    newparamsDict[key] = value

            # Вытаскиваем данные из списков-значений словаря !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            for key, value in paramsAndValues.items():
                #values = value.split(' ')
                newparamsDict[key] = [to_int_if_possible(value)]

            # Запрос к БД
            result = SelectFromDb(**newparamsDict)
            return jsonify(result)
        
    
    except ValidationError as err:
        return jsonify({"errors": err.messages}), 400

    except Exception as e:
       logger.error(f'Ошибка: {e}')
       print(e)
       return jsonify(Error = 'Произошла непредвиденная ошибка'), 400


def SelectFromDb(**kwargs):
    '''Корректирует входные значения и даёт SELECT в БД'''
    print(kwargs)
    partitionTable = globals()["EquipmentInfo_{0}".format(kwargs['year'])]
    kwargs.__delitem__('year') # Удаляю год, тк я уже выбрал нужную партицию для поиска

    # Создаём условия для WHERE
    ANDexpressions = []
    ORexpressions = []

    def replaceSymbols(elList):
        '''Заменяет одни символы на другие'''
        resList = []
        replace_dict = {'*': '%', '?': ' '}
        translation_table = str.maketrans(replace_dict)
        for el in elList:
            resList.append(el.translate(translation_table))
        return resList

    
    items = dict()
    # Итерируем список кортежей
    for item in kwargs.items():
        key = item[0]
        valList = item[1]
        if key != 'start' and key != 'rows':
            # Заменяем спецсимволы на те, что используются в postgresql 
            items[key] = replaceSymbols(valList)
        elif key != 'start' or key != 'rows':
            items[key] = valList

    keysWithoutJoin = ['serialNumber', 'svidetelstvoNumber', 'poverkaDate', 'konecDate', 'isPrigodno']
    keysWithJoin = ['poveritelOrg', 'registerNumber', 'typeName']
    #query = session.query(partitionTable)

    # Пробегаемся по полученным параметрам
    for key, valueArr in items.items():
        if key in keysWithoutJoin:
            column = getattr(partitionTable, key)
            # Пробегаемся по значениям параметра
            for v in valueArr:
                if '*' in v or ' ' in v:
                    ANDexpressions.append(column == v)
                else:
                    ANDexpressions.append(column.ilike(f"{v}"))


        # Если используется неточный поиск по неопределенным параметрам
        elif key == 'search':
            for v in valueArr:
                # Добавляем поиск только по параметрам, которые доступны для неточного поиска
                ORexpressions.append(UniqueTypeNames.typeName.ilike(f"{v}"))
                ORexpressions.append(UniquePoveritelOrgs.poveritelOrg.ilike(f"{v}"))
                ORexpressions.append(UniqueRegisterNumbers.registerNumber.ilike(f"{v}"))
                serCol = getattr(partitionTable, 'serialNumber')
                ORexpressions.append(serCol.ilike(f"{v}"))
                svidCol = getattr(partitionTable, 'svidetelstvoNumber')
                ORexpressions.append(svidCol.ilike(f"{v}"))


        elif key in keysWithJoin:
            if key == 'typeName':
                ANDexpressions.append(UniqueTypeNames.typeName == kwargs['typeName'][0])

            elif key == 'poveritelOrg':
                ANDexpressions.append(UniquePoveritelOrgs.poveritelOrg == kwargs['poveritelOrg'][0])

            elif key == 'registerNumber':
                ANDexpressions.append(UniqueRegisterNumbers.registerNumber == kwargs['registerNumber'][0])

        elif key in ['rows', 'start', 'sort']: # ['mit_title', 'org_title', 'rows', 'start'] !!!!!!!!!!
            continue  # rows и start обработаны ранее, остальные будут обработаны в JOIN и FILTER
        else:
            raise AttributeError(f"Некорректный параметр: {key}")

    # Составляем тело Where условия
    combined_expression1 = and_(*ANDexpressions)
    combined_expression2 = or_(*ORexpressions)
    combined_expression = and_(combined_expression1, combined_expression2)

    query = session.query(partitionTable, UniqueTypeNames, UniquePoveritelOrgs, UniqueRegisterNumbers) \
        .join(UniqueTypeNames, partitionTable.typeNameId == UniqueTypeNames.id) \
        .join(UniquePoveritelOrgs, partitionTable.poveritelOrgId == UniquePoveritelOrgs.id) \
        .join(UniqueRegisterNumbers, partitionTable.registerNumberId == UniqueRegisterNumbers.id) \
        .filter(combined_expression) 

    # Добавляем сортировку, если такой параметр был задан
    if 'sort' in kwargs:
        col = getattr(partitionTable, kwargs['sort'][0])
        if kwargs['sort'][1] == 'desc':
            query = query.order_by(desc(col))
        else:
            query = query.order_by(col)

    query = query.limit(items['rows'][0]) \
        .offset(items['start'][0])

    res = query.all()
    result = [queryToRow(query) for query in res]
    return result


def queryToRow(query):
    '''Преобразует полученный объект в словарь'''
    result = {}
    # Объект может содержать несколько строк, пробегаемся по ним
    for item in query:
        result.update(to_dict(item))
    return result


def to_dict(instance):
    '''Преобразует строку таблицы в словарь'''
    if not instance:
        return {}
    # Получаем типы по их названию
    columns = [column.key for column in class_mapper(instance.__class__).columns]
    return {column: getattr(instance, column) for column in columns}


if __name__ == "__main__":
    app.run(debug=True)
