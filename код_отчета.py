# Импортируем библиотеки
import logging, os, bleach, jwt
from sqlalchemy import Column, BigInteger, VARCHAR, Boolean,Date, create_engine, and_, or_,desc
from sqlalchemy.orm import sessionmaker, declarative_base, class_mapper
from flask_talisman import Talisman
from marshmallow import Schema, fields, validates, ValidationError, validate
from flask import Flask, request, jsonify, make_response, session
from datetime import datetime, timedelta
from functools import wraps
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

app = Flask(__name__)

SECRET_KEY = os.getenv("SECRET_KEY")
CLIENT_KEY = os.getenv("CLIENT_KEY")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Настройка политики CSP
csp = {
    'default-src': "'none'",  # Запрещает выполнение любых скриптов по умолчанию
    'script-src': "'none'",   # Запрещает выполнение JavaScript кода
    'style-src': "'none'",    # Запрещает загрузку стилей
    'img-src': "'none'",      # Запрещает загрузку изображений
    'font-src': "'self'"     # Разрешает шрифты только с текущего домена
}

# Применяем политики CSP и конфигурируем параметры HTTP 
talisman = Talisman(app)

# Подключаемся к базе данных
engine = create_engine(f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@localhost:5432/Arshindb')
# Организуем канал для передачи запросов
Session = sessionmaker(bind=engine)
session = Session()

# Создаём базовый класс для декларативных определений классов
Base = declarative_base()

# Модель базы данных
class EquipmentInfo(Base):
    __tablename__ = 'EquipmentInfo'
    id = Column(BigInteger(), primary_key=True)
    serialNumber = Column(VARCHAR(256))
    svidetelstvoNumber = Column(VARCHAR(256))
    poverkaDate = Column(Date())
    konecDate = Column(Date())
    vri_id = Column(BigInteger())
    isPrigodno = Column(Boolean())
    poveritelOrg = Column(VARCHAR(256))
    typeName = Column(VARCHAR(512))
    registerNumber = Column(VARCHAR(16))


# Настройка логгера
logger = logging.getLogger('arshinAPIlogger')
# Устанавливаем уровень логирования
logger.setLevel(logging.ERROR)

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
    '''Проводит аутентификацию на основе JWT токена'''
    @wraps(func)
    def decorated(*args, **kwargs):
        # Получаем токен из HTTP заголовка
        token = request.headers.get('Authorization')
        # Если токен не найден
        if not token:
            return jsonify({'message': 'Токен был утерян или время его существования закончилось'}), 401
        try:
            # Декодируем токен
            data = jwt.decode(token.split(" ")[1], SECRET_KEY, algorithms=["HS256"])
        except Exception as e:
            logger.error(f'Ошибка: {e}')
            return jsonify({'message': 'Неверный токен', 'error': str(e)}), 403

        return func(*args, **kwargs)
    return decorated


@app.route('/login', methods=['POST'])
def login():
    '''Отвечает за получение пользователем JWT токена по его ключу'''
    auth = request.form
    # Если передан параметр key и он принадлежит конкретному пользователю
    if auth.get('key') == CLIENT_KEY:
        # Генерируем токен, который будет существовать 1 день
        token = jwt.encode({'user': auth.get('username'), 'exp': datetime.utcnow() + timedelta(days=1)}, SECRET_KEY, algorithm='HS256')
        # Возвращаем пользователю токен
        return jsonify({'token': token})
    return make_response('Ваш ключ недействителен', 403, {'WWW-Authenticate': 'Basic realm="Login required!"'})


correctParams = ['vri_id', 'poveritelOrg', 'registerNumber', 'serialNumber', 'svidetelstvoNumber',
                 'poverkaDate', 'konecDate', 'typeName', 'isPrigodno', 'sort', 'start', 'rows', 'search']


class ParamsSchema(Schema):
    '''Схема для валидации'''
    # Возможные параметры и их допустимые значения
    sort = fields.Str()
    vri_id = fields.Int()
    rows = fields.Int(validate=validate.Range(min = 1, max = 100, error = 'Параметр rows может принимать значения от 1 до 100'))
    start = fields.Int(validate=validate.Range(min = 0, max = 99999, error = 'Параметр rows может принимать значения от 0 до 99999'))
    isPrigodno = fields.Str(validate=validate.OneOf(choices=['true', 'false'], error = 'Параметр isPrigodno может принимать значение true или false'))
    svidetelstvoNumber = fields.Str()
    registerNumber = fields.Str()
    serialNumber = fields.Str()
    poverkaDate = fields.Str()
    konecDate = fields.Str()
    poveritelOrg = fields.Str()
    typeName = fields.Str()

    @validates('sort')
    def validate_sort(self, value):
        '''Читает значения для сортировки данных'''
        # Получаем значения до и после знаков-разделителей
        value = value.replace('%20', ' ').replace('+', ' ')
        parts = value.split(' ')
        # Генерируем ошибку если введено более двух слов
        if len(parts) != 2:
            raise ValidationError("Параметр sort при нимает 2 значения разделённых символами %20 или +")   
        else:
            # Параметр sort имеет всего 2 возможных значения
            if parts[-1] not in ['asc', 'desc']:
                raise ValidationError("Параметр sort принимает значение asc или desc")


def sanitize_input(inputDict):
    '''Очищает строку от вредоносного кода'''
    res = dict()
    for k, v in inputDict.items():
        # Добавляем форматированую строку
        res[k] = bleach.clean(v)
    return res


def validation(paramsAndValues):
    '''Валидирует параметры и их значения'''

    params = paramsAndValues.keys()
    invalidParams = {key for key in params if key not in correctParams}

    # Если есть хотя бы один параметр с некорректым названием
    if len(invalidParams) > 0:
        raise ValidationError(dict=invalidParams, message="Были найдены некорректные названия параметров")

    # Очищаем значения от потенциально опасных скриптов
    clearedDict = sanitize_input(paramsAndValues)
    # Валидируем значения параметров
    schema.load(clearedDict)       

    # Если встретились повторяющиеся параметры
    if len(set(paramsAndValues)) != len(paramsAndValues):
        raise ValidationError(dict=invalidParams, message="Некоторые параметры повторяются, используйте & для перечисления значений")

    # Если задан параметр search и ещё один из четких параметров
    elif len([key for key in params if key == 'search']) != 0 and len([key for key in params if key in preciseSearchParams]) != 0:
        raise ValidationError(message="Нельзя использовать поиск по одному параметру и по всем одновременно")
    
    # В остальных случаях валидация пройдена
    else:
        return True

# Параметры недосступные для поиска
impreciseSearchParams = ['rows', 'start', 'sort']
# Параметры доступные для поиска
preciseSearchParams = ['poveritelOrg', 'registerNumber', 'typeName', 'serialNumber',
                        'svidetelstvoNumber', 'poverkaDate', 'konecDate', 'isPrigodno']


schema = ParamsSchema()
@app.route('/vri', methods=['GET'])
@token_required
def vri():
    '''Вызывается пользователем с заданными параметрами'''

    # Активируем конструкцию для перехвата исключений
    try:
        paramsAndValues = request.args.to_dict()

        if validation(paramsAndValues) == True:
            newparamsDict = dict()
            defaultValues = {'rows': [10], 'start': [0]}

            # Добавляем значения по умолчанию 
            for key, value in defaultValues.items():       
                if key not in paramsAndValues:
                    newparamsDict[key] = value

            # Получаем данные из значений словаря
            for key, value in paramsAndValues.items():
                newparamsDict[key] = [to_int_if_possible(value)]

            # Запрашиваем данные из базы
            result = SelectFromDb(**newparamsDict)
            return jsonify(result)
            
    # Обрабатываем исключения
    except ValidationError as err:
        logger.error(f'Ошибка: {e}')
        return jsonify({"Ошибки": err.messages}), 400

    except Exception as e:
       logger.error(f'Ошибка: {e}')
       return jsonify(Error = 'Произошла непредвиденная ошибка'), 400
    

def SelectFromDb(**kwargs):
    '''Корректирует входные значения и даёт SELECT в БД'''

    # Создаём условия для условия WHERE
    ANDexpressions = []
    ORexpressions = []

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


    # Итерируем полуенные параметры
    for key, valueArr in items.items():

        # Если есть параметр search, значит используется полнотекстовый поиск
        # по списку параметров
        if key == 'search':
            i = 0
            for k in preciseSearchParams:
                preciseCol = getattr(EquipmentInfo, k)
                # Дополняем условие ИЛИ
                ORexpressions.append(preciseCol.ilike(f"{valueArr[i]}"))
                i += 1

        elif key in preciseSearchParams:
            column = getattr(EquipmentInfo, key)
            # Итерируем значения параметра
            for v in valueArr:
                # Дополняем условие И
                if '*' in v or ' ' in v:
                    ANDexpressions.append(column == v)
                else:
                    ANDexpressions.append(column.ilike(f"{v}"))

        # rows и start обработаны ранее, остальные будут обработаны в JOIN и FILTER
        elif key in ['rows', 'start', 'sort']:
            continue

    # Составляем тело WHERE условия
    combined_expression1 = and_(*ANDexpressions)
    combined_expression2 = or_(*ORexpressions)
    combined_expression = and_(combined_expression1, combined_expression2)

    # Добавляем SELECT и WHERE
    query = session.query(EquipmentInfo).filter(combined_expression) 

    # Добавляем сортировку, если такой параметр был задан
    if 'sort' in kwargs:
        col = getattr(EquipmentInfo, kwargs['sort'][0])
        # Сортируем по убыванию
        if kwargs['sort'][1] == 'desc':
            query = query.order_by(desc(col))
        # Сортируем по возрастанию
        else:
            query = query.order_by(col)

    # Дополняем условия ограничения выборки
    query = query.limit(items['rows'][0]) \
        .offset(items['start'][0])

    # Получаем результат запроса
    res = query.all()

    # Преобразуем данные в удобочитаемый формат
    result = [to_dict(query) for query in res]
    return result


def to_dict(instance):
    '''Преобразует объект таблицы в словарь'''
    if not instance:
        return {}
    # Получаем типы по их названию
    columns = [column.key for column in class_mapper(instance.__class__).columns]
    return {column: getattr(instance, column) for column in columns}


def replaceSymbols(elList):
    '''Заменяет одни символы на другие'''
    resList = []
    replace_dict = {'*': '%', '?': ' '}
    translation_table = str.maketrans(replace_dict)
    for el in elList:
        resList.append(el.translate(translation_table))
    return resList

def to_int_if_possible(s):
    '''Преобразует строку в целое число, при неудаче возвращает строку'''
    if s.isdigit():
        return int(s)
    return s


if __name__ == "__main__":
    # Запускаем приложение в режиме отладки
    app.run(debug=True)
