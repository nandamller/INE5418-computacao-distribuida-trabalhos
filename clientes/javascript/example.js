const { EncurtadorClient } = require('./client');

(async () => {
  const c = new EncurtadorClient();

  console.log('Testando Encurta:', await c.encurta('https://www.ufsc.br'));

  const res = await c.encurta('https://www.inf.ufsc.br');
  const cod = res.codigo;
  console.log(`Resolvendo ${cod}:`, await c.resolve(cod));
  console.log(`Resolvendo ${cod} de novo (esperado HIT):`, await c.resolve(cod));
})();
