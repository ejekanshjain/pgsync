import { faker } from '@faker-js/faker'
import { mkdir, writeFile } from 'fs/promises'

const getRandomIn = (min, max) =>
  Math.floor(Math.random() * (max - min + 1)) + min

const main = async () => {
  try {
    await mkdir('sql')
  } catch (err) {}
  let brandsString = 'INSERT INTO brands (id, name) VALUES '
  for (let i = 1; i <= 50; i++) {
    if (i !== 1) brandsString += ','
    const name = faker.company.name().replace(/'/g, "''")
    brandsString += `\n('${i}', '${name}')`
  }
  brandsString += ';'
  await writeFile('sql/brands.sql', brandsString)

  let categoriesString = 'INSERT INTO categories (id, name) VALUES '
  for (let i = 1; i <= 20; i++) {
    if (i !== 1) categoriesString += ','
    const name = faker.company.name().replace(/'/g, "''")
    categoriesString += `\n('${i}', '${name}')`
  }
  categoriesString += ';'
  await writeFile('sql/categories.sql', categoriesString)

  let productsString =
    'INSERT INTO products (id, name, price, "brandId") VALUES '
  for (let i = 1; i <= 10000; i++) {
    if (i !== 1) productsString += ','
    const name = faker.commerce.productName().replace(/'/g, "''")
    const price = (Math.random() * 1000).toFixed(2)
    productsString += `\n('${i}', '${name}', ${price}, '${getRandomIn(1, 50)}')`
  }
  productsString += ';'
  await writeFile('sql/products.sql', productsString)

  let productsCategoriesString =
    'INSERT INTO products_categories (id, "productId", "categoryId", "isPrimary") VALUES '
  const uq = {}
  for (let i = 1; i <= 5000; i++) {
    const productId = getRandomIn(1, 10000)
    const categoryId = getRandomIn(1, 20)
    const key = `${productId}-${categoryId}`
    if (uq[key]) continue
    if (i !== 1) productsCategoriesString += ','
    uq[key] = true
    productsCategoriesString += `\n('${i}', '${productId}', '${categoryId}', ${
      Math.random() > 0.2 ? 'TRUE' : 'FALSE'
    })`
  }
  productsCategoriesString += ';'
  await writeFile('sql/products_categories.sql', productsCategoriesString)

  const merged =
    brandsString +
    '\n' +
    categoriesString +
    '\n' +
    productsString +
    '\n' +
    productsCategoriesString
  await writeFile('sql/merged.sql', merged)
}

main()
