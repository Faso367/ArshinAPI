from flask import Flask, request, jsonify
from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, VARCHAR, Boolean, SmallInteger, Date, create_engine, and_, or_,desc
from sqlalchemy.schema import ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, class_mapper
import logging, os

current_year =  datetime.now().year

app = Flask(__name__)

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

class EquipmentInfoPartitioned(Base):
    __tablename__ = 'EquipmentInfoPartitioned'
    id = Column(BigInteger(), primary_key=True)
    registerNumber = Column(VARCHAR(16))
    serialNumber = Column(VARCHAR(256))
    svidetelstvoNumber = Column(VARCHAR(256))
    poverkaDate = Column(Date())
    konecDate = Column(Date())
    vri_id = Column(BigInteger())
    isPrigodno = Column(Boolean())
    poveritelOrgId = Column(Integer(), ForeignKey('UniquePoveritelOrgs.id'))
    typeNameId = Column(Integer(), ForeignKey('UniqueTypeNames.id'))
    year = Column(SmallInteger())

# Будем создавать партиции с такими полями
def create_base_attributes():
    return {
    'id': Column(BigInteger(), primary_key=True),
    'registerNumber': Column(VARCHAR(16)),
    'serialNumber': Column(VARCHAR(256)),
    'svidetelstvoNumber': Column(VARCHAR(256)),
    'poverkaDate': Column(Date()),
    'konecDate': Column(Date()),
    'vri_id': Column(BigInteger()),
    'isPrigodno': Column(Boolean()),
    'poveritelOrgId': Column(Integer(), ForeignKey('UniquePoveritelOrgs.id')),
    'typeNameId': Column(Integer(), ForeignKey('UniqueTypeNames.id')),
    'year': Column(SmallInteger())
    }

# Динамическое создание классов-моделей для партиций
for year in range(2018, current_year + 1):
    class_name = f'EquipmentInfo_{year}'
    tablename = f'EquipmentInfo_{year}'
    # Создаём словарь для полей класса. Так как tablename разный, то мы добавляли сначала его
    attributes = {'__tablename__': tablename}
    # Преобразуем остальные поля в пары и добавляем их
    attributes.update(create_base_attributes())
    # Создаём классы с указанным именем, входным параметром и полями
    globals()[class_name] = type(class_name, (Base,), attributes)


correctParams = ['vri_id', 'poveritelOrg', 'registerNumber', 'serialNumber', 'svidetelstvoNumber',
                 'poverkaDate', 'konecDate', 'typeName', 'isPrigodno',
                 'year', 'sort', 'start', 'rows', 'search']

correctValues = {'rows': range(1, 101), 'start': range(99999), 'isPrigodno': ['true', 'false'], 'year': range(2000, 3000)}


def to_int_if_possible(s):
    if s.isdigit():
        return int(s)
    return s

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


@app.route('/vri', methods=['GET'])
def vri():
    '''Вызывается пользователем с заданными параметрами'''
    try:
        paramsDict = request.args.to_dict()

        # Валидируем на допустимые параметры и числовые значения
        invalid_entries = {key: value for key, value in paramsDict.items()
            if key not in correctParams or to_int_if_possible(value) not in correctValues.get(key, to_int_if_possible(value))}

        # Валидируем значение параметра sort
        if invalid_entries or 'sort' in paramsDict.keys() and paramsDict['sort'].split(' ')[-1] not in ['asc', 'desc']:
            return jsonify(Invalid_data=invalid_entries, Error="Были найдены некорректные параметры")

        elif len(set(paramsDict)) != len(paramsDict):
            return jsonify(Invalid_data=invalid_entries, Error="Некоторые параметры повторяются, используйте & для перечисления значений")

        # Если всё ок
        else:
            newparamsDict = dict()
            defaultValues = {'year': current_year, 'rows': [10], 'start': [0]}

            # Добавляем дефолтные пары ключ-значение, если их значения не заданы 
            for key, value in defaultValues.items():       
                if key not in paramsDict:
                    newparamsDict[key] = value

            # Вытаскиваем данные из списков-значений словаря
            for key, value in paramsDict.items():
                values = value.split(' ')
                newparamsDict[key] = [to_int_if_possible(val) for val in values]

            # Запрос к БД
            result = SelectFromDb(**newparamsDict)
            return jsonify(result)
            
    except Exception as e:
        logger.error(f'Ошибка: {e}')
        return jsonify(Error = 'Произошла непредвиденная ошибка')


def SelectFromDb(**kwargs):
    '''Корректирует входные значения и даёт SELECT в БД'''

    partitionTable = globals()["EquipmentInfo_{0}".format(kwargs['year'][0])]
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
            items[key] = replaceSymbols(valList)
        elif key != 'start' or key != 'rows':
            items[key] = valList

    # Пробегаемся по полученным параметрам
    for key, value in items.items():
        if hasattr(partitionTable, key):
            column = getattr(partitionTable, key)
            if '*' in value or ' ' in value:
                ANDexpressions.append(column == value)
            else:
                ANDexpressions.append(column.ilike(f"{value}"))

        # Если используется неточный поиск по неопределенным параметрам
        elif key == 'search':
            for v in value:
                ORexpressions.append(UniqueTypeNames.typeName.ilike(f"{v}"))
                ORexpressions.append(UniquePoveritelOrgs.poveritelOrg.ilike(f"{v}"))
                # Доступ к колонке registerNumber через getattr
                regCol = getattr(partitionTable, 'registerNumber')
                ORexpressions.append(regCol.ilike(f"{v}"))
                serCol = getattr(partitionTable, 'serialNumber')
                ORexpressions.append(serCol.ilike(f"{v}"))
                svidCol = getattr(partitionTable, 'svidetelstvoNumber')
                ORexpressions.append(svidCol.ilike(f"{v}"))

        elif key in ['typeName', 'poveritelOrg', 'rows', 'start', 'sort']: # ['mit_title', 'org_title', 'rows', 'start'] !!!!!!!!!!
            continue  # rows и start обработаны ранее, остальные будут обработаны в JOIN и FILTER
        else:
            raise AttributeError(f"Некорректный параметр: {key}")

    # Дополняем условия
    if 'typeName' in kwargs:
        ANDexpressions.append(UniqueTypeNames.typeName == kwargs['typeName'])
    elif 'poveritelOrg' in kwargs:
        ANDexpressions.append(UniquePoveritelOrgs.poveritelOrg == kwargs['poveritelOrg'])
    
    # Составляем тело Where условия
    combined_expression1 = and_(*ANDexpressions)
    combined_expression2 = or_(*ORexpressions)
    combined_expression = and_(combined_expression1, combined_expression2)
     
    query = session.query(partitionTable, UniqueTypeNames, UniquePoveritelOrgs) \
        .join(UniqueTypeNames, partitionTable.typeNameId == UniqueTypeNames.id) \
        .join(UniquePoveritelOrgs, partitionTable.poveritelOrgId == UniquePoveritelOrgs.id) \
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


'''
@app.get("/svidetelstvo<string:svidetelstvo>&year=<int:year>&rows=<int:rows>")
def GetBysvidetelstvoNumber(orgName, year, rows):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(GET_EQUIPMENT_BY_ORG_AND_YEAR, (year, year, orgName))
            results = cursor.fetchall()
            print(results)
            #mas = results.spl
            column_names = [desc[0] for desc in cursor.description]
            
            # Convert the results to a list of dictionaries
            data = [dict(zip(column_names, row)) for row in results]
            print(data)

    return data
    #return data

    #return json.loads(f'"{data}"')
'''