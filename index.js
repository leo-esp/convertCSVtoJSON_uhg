

const fs = require('fs');
const csv = require('fast-csv');
const _ = require('lodash');
var baseDados = [];
var hospitaisDescredenciados = [];
var hospitaisIndicados = [];
var planos = [];
var i = 0;

fs.createReadStream('finalx11.csv', {encoding: 'latin1'})
    .pipe(csv({ delimiter: ';', objectMode: true, headers: true }))
    .on('data', (row) => {
        console.log(`${++i} processed lines`);
        if (row.Atende=== 'S') {
            if(hospitaisDescredenciados.filter(h => h.hospital_descredenciado_code === row['Codigo Hospital Descredenciado']).length === 0 ){
                hospitaisDescredenciados.push({
                    hospital_descredenciado_code: row['Codigo Hospital Descredenciado'],
                    hospital_descredenciado: row['Nome Hospital Descredenciado'],
                })
            }
            if(hospitaisIndicados.filter(h => h.hospital_indicado_code === row['Codigo Hospital Indicado']).length === 0 ){
                hospitaisIndicados.push({
                    hospital_indicado_code: row['Codigo Hospital Indicado'], 
                    nome: row['Nome Hospital Indicado'],
                    endereco: row['Endereço Hospital Indicado'],
                    bairro: row['Bairro Hospital Indicado'],
                    cidade: row['Cidade Hospital Indicado'],
                    estado: row['Estado Hospital Indicado'],
                    site: row['Site Hospital Indicado'],
                    telefone: row['Telefone Hospital Indicado'],
                })
            }
            if(planos.filter(p => p.codigoRede === row['Plano Codigo Rede Hospital Indicado']).length === 0 ){
                planos.push({
                    codigoRede: row['Plano Codigo Rede Hospital Indicado'],
                    nomePlano: row['Plano Nome Hospital Indicado'],
                    operadora: row['Plano Operadora Hospital Indicado '],
                    nivel: row['Plano Nivel Hospital Indicado']
                })
            }
            if (baseDados.filter(hosp => hosp.hospital_descredenciado_code === row['Codigo Hospital Descredenciado']).length === 0) {
                baseDados.push({
                    hospital_descredenciado_code: row['Codigo Hospital Descredenciado'],
                    hospital_descredenciado: row['Nome Hospital Descredenciado'],
                    hospital_indicado: [{
                        hospital_indicado_code: row['Codigo Hospital Indicado'], 
                        nome: row['Nome Hospital Indicado'],
                        endereco: row['Endereço Hospital Indicado'],
                        bairro: row['Bairro Hospital Indicado'],
                        cidade: row['Cidade Hospital Indicado'],
                        estado: row['Estado Hospital Indicado'],
                        site: row['Site Hospital Indicado'],
                        telefone: row['Telefone Hospital Indicado'],
                        planos: [{
                            codigoRede: row['Plano Codigo Rede Hospital Indicado'],
                            nomePlano: row['Plano Nome Hospital Indicado'],
                            operadora: row['Plano Operadora Hospital Indicado '],
                            nivel: row['Plano Nivel Hospital Indicado']
                        }]
                    }]
                });
            } else {
                let hospitalCredenciado = baseDados.filter(hosp => hosp.hospital_descredenciado_code === row['Codigo Hospital Descredenciado'])[0];
                if (hospitalCredenciado.hospital_indicado.filter(indc => indc.hospital_indicado_code === row['Codigo Hospital Indicado']).length === 0) {
                    baseDados.filter(hosp => hosp.hospital_descredenciado_code === row['Codigo Hospital Descredenciado'])[0].hospital_indicado.push({
                        hospital_indicado_code: row['Codigo Hospital Indicado'], 
                        nome: row['Nome Hospital Indicado'],
                        endereco: row['Endereço Hospital Indicado'],
                        bairro: row['Bairro Hospital Indicado'],
                        cidade: row['Cidade Hospital Indicado'],
                        estado: row['Estado Hospital Indicado'],
                        site: row['Site Hospital Indicado'],
                        telefone: row['Telefone Hospital Indicado'],
                        planos: [{
                            codigoRede: row['Plano Codigo Rede Hospital Indicado'],
                            nomePlano: row['Plano Nome Hospital Indicado'],
                            operadora: row['Plano Operadora Hospital Indicado '],
                            nivel: row['Plano Nivel Hospital Indicado']
                        }]
                    });
                }else{
                    baseDados.filter(hosp => hosp.hospital_descredenciado_code === row['Codigo Hospital Descredenciado'])[0].hospital_indicado.filter(indc => indc.hospital_indicado_code === row['Codigo Hospital Indicado'])[0].planos.push({
                        codigoRede: row['Plano Codigo Rede Hospital Indicado'],
                        nomePlano: row['Plano Nome Hospital Indicado'],
                        operadora: row['Plano Operadora Hospital Indicado '],
                        nivel: row['Plano Nivel Hospital Indicado']
                    })
                }
            }
        }
    })
    .on('end', function (data) {
        console.log('Read Finished')
        var jsonDados = JSON.stringify(baseDados);
        var jsonHospitaisDescredenciados = JSON.stringify(hospitaisDescredenciados);
        var jsonHospitaisIndicados = JSON.stringify(hospitaisIndicados);
        var jsonPlanos = JSON.stringify(planos);
        fs.writeFile('11_base-de-dados.json', jsonDados, 'utf8', () => { });
        fs.writeFile('11_hospitais-descredenciados.json', jsonHospitaisDescredenciados, 'utf8', () => { });
        fs.writeFile('11_hospitais-indicados.json', jsonHospitaisIndicados, 'utf8', () => { });
        fs.writeFile('11_planos.json', jsonPlanos, 'utf8', () => { });
    });






