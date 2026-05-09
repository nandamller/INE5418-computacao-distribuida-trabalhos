const { EncurtadorClient } = require('./client');

(async () => {
  const c = new EncurtadorClient();

  console.log('Testando Encurta:', await c.encurta('https://www.outlook.live.com'));

  console.log('Testando Encurta:', await c.encurta('https://www.hotmail.com'));

  const res1 = await c.encurta('https://www.gmail.com');
  const cod1 = res1.codigo;
  console.log(`Resolvendo ${cod1}:`, await c.resolve(cod1));
  console.log(`Resolvendo ${cod1} de novo (esperado HIT):`, await c.resolve(cod1));

  const res2 = await c.encurta('https://mail.yahoo.com/');
  const cod2 = res2.codigo;
  console.log(`Resolvendo ${cod2}:`, await c.resolve(cod2));
  console.log(`Resolvendo ${cod2} de novo (esperado HIT):`, await c.resolve(cod2));

  // Remoção e invalidação de cache: depois do remove, o resolve deve voltar
  // a consultar o servidor (cache miss) e receber 404, provando que o proxy
  // invalidou a entrada local.
  console.log(`Removendo ${cod1}:`, await c.removeUrl(cod1));
  console.log(`Resolvendo ${cod1} apos remover (esperado erro):`, await c.resolve(cod1));

})();
