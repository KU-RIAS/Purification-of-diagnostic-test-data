# 진단 검사 데이터 정제 프로그램


## Introduction

#### 병원에서, 수천가지의 다양한 검사가 존재하는 진단검사 결과는 막대한 양의 빅데이터로써 존재한다. 이 데이터는 검사 기기별로 표기 형식이 다양하고, 일부 수작업을 통해 이루어 지기도 하여, 데이터 형식의 일관성이 깨지게 되고, 결국 비정형 텍스트 데이터 형태로 저장되어 있는 실정이다.
#### 본 프로그램은 이러한 문제를 해결하기 위하여, 룰 기반의 알고리즘을 통해 자동으로 빠른 시간 안에 타입 별 데이터 베이스를 구축하고, 필요시 사용자가 원하는 용어 및 코드로 전환하는 방법 및 시스템을 개발하고자 한다. 
#

## 제공하는 기능

### - 데이터 정제

### - 정제된 데이터 검증

### - 데이터 정제에 사용되는 filter 수정  
#

## 사용 방법

conda 환경 추천

python version = 3.8

    pip install -r requirements.txt

#

### 1-1. 데이터 정제 (run.py)

Data 디렉토리의 config.ini 파일 설정

* [Database] 항목의 요소들은 터치하지 말것 (데이터베이스 연동 미구현)
* [data] 항목의 chunk_size는 한번에 처리할 행의 갯수 설정

#

Data/Filter 디렉토리의 filters.csv 파일 확인

* column명을 변경하지 말것

#

Data/Output 디렉토리가 비어있는지 확인

* input 파일을 정제할때 output 디렉토리는 비어있어야 함

#

Data/Input 디렉토리의 INPUT.csv 파일 확인

* 여러개의 input file을 넣어도 됨 (input1.csv, inputA.csv, inputZ.csv ....)

#

실행 코드

    python run.py -c ./Data/config.ini -d False 

생성되는 결과는 Data/Output에 저장됨

#

### 1-2. filter 내용

적용되는 순서: 왼쪽 컬럼에서 오른쪽 컬럼으로, 각 컬럼은 위에서 아래 순서로 적용.

컬럼 명에서 pm은 partially match / fm은 fully match의 의미로 작성. 

필터 내용은 기본적인 문자열과 정규표현식을 지원.

추가적으로 특별한 약속 구현(공백, |기호)

#

case1. 공백

필터에 공백을 사용시, 필터링 된 내용의 저장될 모습을 정할 수 있다.
예를들어 l/kg를 필터링하여 L/kg로 저장하고 싶은 경우,
필터에 l/kg L/kg 형태로 입력한다.

#

case2. | 기호

'또는'의 의미를 부여할 수 있다.
예를들어 pos|양성 의 의미는 input 데이터에 pos 또는 양성이 있다면 그것을 필터링 하겠다는 의미이다.

#

case3. 응용

공백과 | 기호를 혼용하여 사용한다면 아래와 같이 사용할 수 있다.

오타를 잡는 용도: elevated|elavated elevated

또는으로 걸러진 내용에 대해 저장 형태 지정: ab|ab[+]|a|a[+]|b|b[+]|o|o+ AB AB+ A A+ B B+ O O+

#

### 1-3 output data에 대한 검증(verify.py)

    python verify.py

위 코드를 통해 Data/Output 디렉토리에 존재하는 output data에 대한 결과 검증 데이터를 생성해줌.
결과는 Data/Verify 디렉토리에 저장됨.

생성된 데이터를 참고하여 filter를 조정



