const fs = require('fs');

const jsonFile = fs.readFileSync('./yeardream_user.json', 'utf8');
const jsonData = JSON.parse(jsonFile);

let userData = [];
for (const [_, val] of Object.entries(jsonData)) {
  userData = [...userData, ...val];
}

let userList = new Map();

userData.forEach((d) => {
  if (!userList.has(d.id)) {
    userList.set(d.id, {
      real_name: d['real_name'],
      display_name: d['profile']['display_name'],
      email: d['profile']['email'],
      is_admin: d['is_admin'],
      is_bot: d['is_bot']
    });
  }
});

const excelData = [
  [
    '유니크 아이디',
    '실제이름',
    '디스플레이 이름',
    '직업',
    '이메일',
    '어드민여부',
    '봇 여부'
  ]
];

for (let [key, val] of userList) {
  excelData.push([
    key,
    val['real_name'],
    val['display_name'],
    val['email'],
    val['is_admin'],
    val['is_bot']
  ]);
}

const xlsx = require('xlsx');

const workbook = xlsx.utils.book_new();

// 워크시트 생성
const worksheet = xlsx.utils.aoa_to_sheet(excelData);

// 워크북에 워크시트 추가
xlsx.utils.book_append_sheet(workbook, worksheet, 'Sheet1');

// 파일 저장
const filename = '이어드림_유저명단.xlsx';
xlsx.writeFile(workbook, filename);
