CREATE TABLE brands (
    id text,
    name text NOT NULL,
    "isActive" boolean NOT NULL DEFAULT TRUE,
    "createdAt" timestamp with time zone NOT NULL DEFAULT now(),
    "updatedAt" timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT pk_brands PRIMARY KEY (id),
    CONSTRAINT uq_brands_name UNIQUE (name)
);

CREATE TABLE categories (
    id text,
    name text NOT NULL,
    "isActive" boolean NOT NULL DEFAULT TRUE,
    "createdAt" timestamp with time zone NOT NULL DEFAULT now(),
    "updatedAt" timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT pk_categories PRIMARY KEY (id),
    CONSTRAINT uq_categories_name UNIQUE (name)
);

CREATE TABLE products (
    id text,
    name text NOT NULL,
    "isActive" boolean NOT NULL DEFAULT TRUE,
    price double precision NOT NULL,
    popularity integer,
    "brandId" text,
    "createdAt" timestamp with time zone NOT NULL DEFAULT now(),
    "updatedAt" timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT pk_products PRIMARY KEY (id),
    CONSTRAINT fk_products_brandid FOREIGN KEY ("brandId") REFERENCES public.brands (id) ON DELETE CASCADE
);

CREATE TABLE products_categories (
    id text,
    "productId" text,
    "categoryId" text,
    "isPrimary" boolean NOT NULL DEFAULT FALSE,
    "createdAt" timestamp with time zone NOT NULL DEFAULT now(),
    "updatedAt" timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT pk_products_categories PRIMARY KEY (id),
    CONSTRAINT uq_products_categories UNIQUE ("productId", "categoryId"),
    CONSTRAINT fk_products_categories_productid FOREIGN KEY ("productId") REFERENCES public.products (id) ON DELETE CASCADE,
    CONSTRAINT fk_products_categories_categoryid FOREIGN KEY ("categoryId") REFERENCES public.categories (id) ON DELETE CASCADE
);